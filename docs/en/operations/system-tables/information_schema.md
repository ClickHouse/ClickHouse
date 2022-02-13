# INFORMATION_SCHEMA {#information-schema}

`INFORMATION_SCHEMA` (`information_schema`) is a system database that contains views. Using these views, you can get information about the metadata of database objects. These views read data from the columns of the [system.columns](../../operations/system-tables/columns.md), [system.databases](../../operations/system-tables/databases.md) and [system.tables](../../operations/system-tables/tables.md) system tables.

The structure and composition of system tables may change in different versions of the product, but the support of the `information_schema` makes it possible to change the structure of system tables without changing the method of access to metadata. Metadata requests do not depend on the DBMS used.

``` sql
SHOW TABLES FROM INFORMATION_SCHEMA;
```

``` text
┌─name─────┐
│ COLUMNS  │
│ SCHEMATA │
│ TABLES   │
│ VIEWS    │
└──────────┘
```

`INFORMATION_SCHEMA` contains the following views:

-   [COLUMNS](#columns)
-   [SCHEMATA](#schemata)
-   [TABLES](#tables)
-   [VIEWS](#views)

## COLUMNS {#columns}

Contains columns read from the [system.columns](../../operations/system-tables/columns.md) system table and columns that are not supported in ClickHouse or do not make sense (always `NULL`), but must be by the standard.

Columns:

-   `table_catalog` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
-   `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
-   `table_name` ([String](../../sql-reference/data-types/string.md)) — Table name.
-   `column_name` ([String](../../sql-reference/data-types/string.md)) — Column name.
-   `ordinal_position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Ordinal position of a column in a table starting with 1.
-   `column_default` ([String](../../sql-reference/data-types/string.md)) — Expression for the default value, or an empty string if it is not defined.
-   `is_nullable` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the column type is `Nullable`.
-   `data_type` ([String](../../sql-reference/data-types/string.md)) — Column type.
-   `character_maximum_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum length in bytes for binary data, character data, or text data and images. In ClickHouse makes sense only for `FixedString` data type. Otherwise, the `NULL` value is returned.
-   `character_octet_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum length in bytes for binary data, character data, or text data and images. In ClickHouse makes sense only for `FixedString` data type. Otherwise, the `NULL` value is returned.
-   `numeric_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Accuracy of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse it is bitness for integer types and decimal precision for `Decimal` types. Otherwise, the `NULL` value is returned.
-   `numeric_precision_radix` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The base of the number system is the accuracy of approximate numeric data, exact numeric data, integer data or monetary data. In ClickHouse it's 2 for integer types and 10 for `Decimal` types. Otherwise, the `NULL` value is returned.
-   `numeric_scale` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The scale of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse makes sense only for `Decimal` types. Otherwise, the `NULL` value is returned.
-   `datetime_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Decimal precision of `DateTime64` data type. For other data types, the `NULL` value is returned.
-   `character_set_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `character_set_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `character_set_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `collation_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `collation_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `collation_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `domain_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `domain_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `domain_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.

**Example**

Query:

``` sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema=currentDatabase() OR table_schema='') AND table_name NOT LIKE '%inner%' LIMIT 1 FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
table_catalog:            default
table_schema:             default
table_name:               describe_example
column_name:              id
ordinal_position:         1
column_default:
is_nullable:              0
data_type:                UInt64
character_maximum_length: ᴺᵁᴸᴸ
character_octet_length:   ᴺᵁᴸᴸ
numeric_precision:        64
numeric_precision_radix:  2
numeric_scale:            0
datetime_precision:       ᴺᵁᴸᴸ
character_set_catalog:    ᴺᵁᴸᴸ
character_set_schema:     ᴺᵁᴸᴸ
character_set_name:       ᴺᵁᴸᴸ
collation_catalog:        ᴺᵁᴸᴸ
collation_schema:         ᴺᵁᴸᴸ
collation_name:           ᴺᵁᴸᴸ
domain_catalog:           ᴺᵁᴸᴸ
domain_schema:            ᴺᵁᴸᴸ
domain_name:              ᴺᵁᴸᴸ
```

## SCHEMATA {#schemata}

Contains columns read from the [system.databases](../../operations/system-tables/databases.md) system table and columns that are not supported in ClickHouse or do not make sense (always `NULL`), but must be by the standard.

Columns:

-   `catalog_name` ([String](../../sql-reference/data-types/string.md)) — The name of the database.
-   `schema_name` ([String](../../sql-reference/data-types/string.md)) — The name of the database.
-   `schema_owner` ([String](../../sql-reference/data-types/string.md)) — Schema owner name, always `'default'`.
-   `default_character_set_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `default_character_set_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `default_character_set_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
-   `sql_path` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.

**Example**

Query:

``` sql
SELECT * FROM information_schema.schemata WHERE schema_name ILIKE 'information_schema' LIMIT 1 FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
catalog_name:                  INFORMATION_SCHEMA
schema_name:                   INFORMATION_SCHEMA
schema_owner:                  default
default_character_set_catalog: ᴺᵁᴸᴸ
default_character_set_schema:  ᴺᵁᴸᴸ
default_character_set_name:    ᴺᵁᴸᴸ
sql_path:                      ᴺᵁᴸᴸ
```

## TABLES {#tables}

Contains columns read from the [system.tables](../../operations/system-tables/tables.md) system table.

Columns:

-   `table_catalog` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
-   `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
-   `table_name` ([String](../../sql-reference/data-types/string.md)) — Table name.
-   `table_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Table type. Possible values:
    -   `BASE TABLE`
    -   `VIEW`
    -   `FOREIGN TABLE`
    -   `LOCAL TEMPORARY`
    -   `SYSTEM VIEW`

**Example**

Query:

``` sql
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%' LIMIT 1 FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
table_catalog: default
table_schema:  default
table_name:    describe_example
table_type:    BASE TABLE
```

## VIEWS {#views}

Contains columns read from the [system.tables](../../operations/system-tables/tables.md) system table, when the table engine [View](../../engines/table-engines/special/view.md) is used.

Columns:

-   `table_catalog` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
-   `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
-   `table_name` ([String](../../sql-reference/data-types/string.md)) — Table name.
-   `view_definition` ([String](../../sql-reference/data-types/string.md)) — `SELECT` query for view.
-   `check_option` ([String](../../sql-reference/data-types/string.md)) — `NONE`, no checking.
-   `is_updatable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, the view is not updated.
-   `is_insertable_into` ([Enum8](../../sql-reference/data-types/enum.md)) — Shows whether the created view is [materialized](../../sql-reference/statements/create/view/#materialized). Possible values:
    -   `NO` — The created view is not materialized.
    -   `YES` — The created view is materialized.
-   `is_trigger_updatable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, the trigger is not updated.
-   `is_trigger_deletable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, the trigger is not deleted.
-   `is_trigger_insertable_into` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, no data is inserted into the trigger.

**Example**

Query:

``` sql
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
CREATE MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM system.one;
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase() LIMIT 1 FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
table_catalog:              default
table_schema:               default
table_name:                 mv
view_definition:            SELECT * FROM system.one
check_option:               NONE
is_updatable:               NO
is_insertable_into:         YES
is_trigger_updatable:       NO
is_trigger_deletable:       NO
is_trigger_insertable_into: NO
```
