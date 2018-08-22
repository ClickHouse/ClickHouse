# Roadmap

## Q3 2018

- `ALTER UPDATE` for mass changes to data using an approach similar to `ALTER DELETE`
- Adding Protobuf and Parquet to the range of supported input/output formats
- Improved compatibility with Tableau and other business analytics tools

## Q4 2018

- JOIN syntax that conforms to the SQL standard:
    - Multiple `JOIN`s in a single `SELECT`
    - Setting the relationship between tables via `ON`
    - Ability to refer to the table name instead of using a subquery

- Improvements in the performance of JOIN:
    - Distributed JOIN not limited by RAM
    - Transferring predicates that depend on only one side via JOIN

- Resource pools for more accurate distribution of cluster capacity between its users

