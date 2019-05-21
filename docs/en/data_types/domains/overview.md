# Domains

Domains are special-purpose types, that add some extra features atop of existing base type, leaving on-wire and on-disc format of underlying table intact. At the moment, ClickHouse does not support user-defined domains.

You can use domains anywhere corresponding base type can be used:

* Create a column of domain type
* Read/write values from/to domain column
* Use it as index if base type can be used as index
* Call functions with values of domain column
* etc.

### Extra Features of Domains

* Explicit column type name in `SHOW CREATE TABLE` or `DESCRIBE TABLE`
* Input from human-friendly format with `INSERT INTO domain_table(domain_column) VALUES(...)`
* Output to human-friendly format for `SELECT domain_column FROM domain_table`
* Loading data from external source in human-friendly format: `INSERT INTO domain_table FORMAT CSV ...`

### Limitations

* Can't convert index column of base type to domain type via `ALTER TABLE`.
* Can't implicitly convert string values into domain values when inserting data from another column or table.
* Domain adds no constrains on stored values.

[Original article](https://clickhouse.yandex/docs/en/data_types/domains/overview) <!--hide-->