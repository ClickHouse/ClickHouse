# Roadmap

## Q3 2018

- `ALTER UPDATE` for batch changing the data with approach similar to `ALTER DELETE`
- Protobuf and Parquet input and output formats
- Improved compatibility with Tableau and other BI tools

## Q4 2018

- JOIN syntax compatible with SQL standard:
    - Mutliple `JOIN`s in single `SELECT`
    - Connecting tables with `ON`
    - Support table reference instead of subquery

- JOIN execution improvements:
    - Distributed join not limited by memory
    - Predicate pushdown through join

- Resource pools for more precise distribution of cluster capacity between users
