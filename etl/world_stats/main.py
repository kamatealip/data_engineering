"""
Tiny ETL Pipeline #1: World Development Stats
Extract  -> download a public CSV (country/year population, life expectancy, GDP)
Transform -> clean columns, compute derived metrics, aggregate by continent+year
Load     -> write both the cleaned table and the aggregate table into SQLite
No orchestration framework -- just plain Python + pandas, run top to bottom.
"""

import sqlite3
import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv"
DB_PATH = "world_stats.db"


def extract() -> pd.DataFrame:
    """Pull the raw CSV straight into a DataFrame."""
    df = pd.read_csv(DATA_URL)
    print(f"Extracted {len(df)} rows, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean the raw data and derive a couple of new columns/tables."""
    df = df.dropna(subset=["country", "year", "pop", "gdpPercap", "lifeExp"]).copy()

    # Derived metric: total GDP = per-capita GDP * population
    df["gdp_total"] = df["gdpPercap"] * df["pop"]

    # Simple business rule: flag high-income country-years
    df["is_high_income"] = df["gdpPercap"] > 20000

    # Rename to consistent snake_case
    df = df.rename(columns={"lifeExp": "life_exp", "gdpPercap": "gdp_per_capita"})

    # Round numeric country-level metrics
    df["gdp_per_capita"] = df["gdp_per_capita"].round(2)
    df["gdp_total"] = df["gdp_total"].round(2)
    df["life_exp"] = df["life_exp"].round(2)

    country_year = df[[
        "country", "continent", "year", "pop", "life_exp",
        "gdp_per_capita", "gdp_total", "is_high_income"
    ]]

    # A second, aggregated table: avg life expectancy & gdp per continent per year
    continent_year = (
        df.groupby(["continent", "year"], as_index=False)
          .agg(avg_life_exp=("life_exp", "mean"),
               avg_gdp_per_capita=("gdp_per_capita", "mean"),
               total_population=("pop", "sum"))
    )
    continent_year["avg_gdp_per_capita"] = continent_year["avg_gdp_per_capita"].round(2)
    continent_year['avg_life_exp'] = continent_year['avg_life_exp'].round(2)

    print(f"Transformed -> country_year: {len(country_year)} rows, "
          f"continent_year: {len(continent_year)} rows")
    return country_year, continent_year


def load(country_year: pd.DataFrame, continent_year: pd.DataFrame) -> None:
    """Write both tables into a local SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    country_year.to_sql("country_year_stats", conn, if_exists="replace", index=False)
    continent_year.to_sql("continent_year_avg", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Loaded into {DB_PATH}: country_year_stats, continent_year_avg")


def run():
    raw = extract()
    country_year, continent_year = transform(raw)
    load(country_year, continent_year)


if __name__ == "__main__":
    run()