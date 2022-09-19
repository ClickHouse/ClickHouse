---
sidebar_position: 56
sidebar_label: Domains
---

# Domains

Domains are special-purpose types that add some extra features atop of existing base type, but leaving on-wire and on-disc format of the underlying data type intact. At the moment, ClickHouse does not support user-defined domains.

You can use domains anywhere corresponding base type can be used, for example:

-   Create a column of a domain type
-   Read/write values from/to domain column
-   Use it as an index if a base type can be used as an index
-   Call functions with values of domain column

### Extra Features of Domains

-   Explicit column type name in `SHOW CREATE TABLE` or `DESCRIBE TABLE`
-   Input from human-friendly format with `INSERT INTO domain_table(domain_column) VALUES(...)`
-   Output to human-friendly format for `SELECT domain_column FROM domain_table`
-   Loading data from an external source in the human-friendly format: `INSERT INTO domain_table FORMAT CSV ...`

### Limitations

-   Can’t convert index column of base type to domain type via `ALTER TABLE`.
-   Can’t implicitly convert string values into domain values when inserting data from another column or table.
-   Domain adds no constrains on stored values.

[Original article](https://clickhouse.com/docs/en/data_types/domains/) <!--hide-->
