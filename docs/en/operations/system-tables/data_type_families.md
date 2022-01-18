# system.data_type_families {#system_tables-data_type_families}

Contains information about supported [data types](../../sql-reference/data-types/index.md).

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — Data type name.
-   `case_insensitive` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Property that shows whether you can use a data type name in a query in case insensitive manner or not. For example, `Date` and `date` are both valid.
-   `alias_to` ([String](../../sql-reference/data-types/string.md)) — Data type name for which `name` is an alias.

**Example**

``` sql
SELECT * FROM system.data_type_families WHERE alias_to = 'String'
```

``` text
┌─name───────┬─case_insensitive─┬─alias_to─┐
│ LONGBLOB   │                1 │ String   │
│ LONGTEXT   │                1 │ String   │
│ TINYTEXT   │                1 │ String   │
│ TEXT       │                1 │ String   │
│ VARCHAR    │                1 │ String   │
│ MEDIUMBLOB │                1 │ String   │
│ BLOB       │                1 │ String   │
│ TINYBLOB   │                1 │ String   │
│ CHAR       │                1 │ String   │
│ MEDIUMTEXT │                1 │ String   │
└────────────┴──────────────────┴──────────┘
```

**See Also**

-   [Syntax](../../sql-reference/syntax.md) — Information about supported syntax.

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/data_type_families) <!--hide-->
