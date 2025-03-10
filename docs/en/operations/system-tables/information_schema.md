---
slug: /en/operations/system-tables/information_schema
---
# INFORMATION_SCHEMA

`INFORMATION_SCHEMA` (or: `information_schema`) is a system database which provides a (somewhat) standardized, [DBMS-agnostic view](https://en.wikipedia.org/wiki/Information_schema) on metadata of database objects. The views in `INFORMATION_SCHEMA` are generally inferior to normal system tables but tools can use them to obtain basic information in a cross-DBMS manner. The structure and content of views in `INFORMATION_SCHEMA` is supposed to evolves in a backwards-compatible way, i.e. only new functionality is added but existing functionality is not changed or removed. In terms of internal implementation, views in `INFORMATION_SCHEMA` usually map to to normal system tables like [system.columns](../../operations/system-tables/columns.md), [system.databases](../../operations/system-tables/databases.md) and [system.tables](../../operations/system-tables/tables.md).

``` sql
SHOW TABLES FROM INFORMATION_SCHEMA;

-- or:
SHOW TABLES FROM information_schema;
```

``` text
┌─name────────────────────┐
│ COLUMNS                 │
│ KEY_COLUMN_USAGE        │
│ REFERENTIAL_CONSTRAINTS │
│ SCHEMATA                │
| STATISTICS              |
│ TABLES                  │
│ VIEWS                   │
│ columns                 │
│ key_column_usage        │
│ referential_constraints │
│ schemata                │
| statistics              |
│ tables                  │
│ views                   │
└─────────────────────────┘
```

`INFORMATION_SCHEMA` contains the following views:

- [COLUMNS](#columns)
- [KEY_COLUMN_USAGE](#key_column_usage)
- [REFERENTIAL_CONSTRAINTS](#referential_constraints)
- [SCHEMATA](#schemata)
- [STATISTICS](#statistics)
- [TABLES](#tables)
- [VIEWS](#views)

Case-insensitive equivalent views, e.g. `INFORMATION_SCHEMA.columns` are provided for reasons of compatibility with other databases. The same applies to all the columns in these views - both lowercase (for example, `table_name`) and uppercase (`TABLE_NAME`) variants are provided.

## COLUMNS {#columns}

Contains columns read from the [system.columns](../../operations/system-tables/columns.md) system table and columns that are not supported in ClickHouse or do not make sense (always `NULL`), but must be by the standard.

Columns:

- `table_catalog` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
- `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
- `table_name` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `column_name` ([String](../../sql-reference/data-types/string.md)) — Column name.
- `ordinal_position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Ordinal position of a column in a table starting with 1.
- `column_default` ([String](../../sql-reference/data-types/string.md)) — Expression for the default value, or an empty string if it is not defined.
- `is_nullable` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the column type is `Nullable`.
- `data_type` ([String](../../sql-reference/data-types/string.md)) — Column type.
- `character_maximum_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum length in bytes for binary data, character data, or text data and images. In ClickHouse makes sense only for `FixedString` data type. Otherwise, the `NULL` value is returned.
- `character_octet_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum length in bytes for binary data, character data, or text data and images. In ClickHouse makes sense only for `FixedString` data type. Otherwise, the `NULL` value is returned.
- `numeric_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Accuracy of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse it is bit width for integer types and decimal precision for `Decimal` types. Otherwise, the `NULL` value is returned.
- `numeric_precision_radix` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The base of the number system is the accuracy of approximate numeric data, exact numeric data, integer data or monetary data. In ClickHouse it's 2 for integer types and 10 for `Decimal` types. Otherwise, the `NULL` value is returned.
- `numeric_scale` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The scale of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse makes sense only for `Decimal` types. Otherwise, the `NULL` value is returned.
- `datetime_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Decimal precision of `DateTime64` data type. For other data types, the `NULL` value is returned.
- `character_set_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `character_set_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `character_set_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `collation_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `collation_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `collation_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `domain_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `domain_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `domain_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `extra` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `STORED GENERATED` for `MATERIALIZED`-type columns, `VIRTUAL GENERATED` for `ALIAS`-type columns, `DEFAULT_GENERATED` for `DEFAULT`-type columns, or `NULL`.

**Example**

Query:

``` sql
SELECT table_catalog,
       table_schema,
       table_name,
       column_name,
       ordinal_position,
       column_default,
       is_nullable,
       data_type,
       character_maximum_length,
       character_octet_length,
       numeric_precision,
       numeric_precision_radix,
       numeric_scale,
       datetime_precision,
       character_set_catalog,
       character_set_schema,
       character_set_name,
       collation_catalog,
       collation_schema,
       collation_name,
       domain_catalog,
       domain_schema,
       domain_name,
       column_comment,
       column_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE (table_schema = currentDatabase() OR table_schema = '')
  AND table_name NOT LIKE '%inner%' 
LIMIT 1 
FORMAT Vertical;
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

- `catalog_name` ([String](../../sql-reference/data-types/string.md)) — The name of the database.
- `schema_name` ([String](../../sql-reference/data-types/string.md)) — The name of the database.
- `schema_owner` ([String](../../sql-reference/data-types/string.md)) — Schema owner name, always `'default'`.
- `default_character_set_catalog` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `default_character_set_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `default_character_set_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.
- `sql_path` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — `NULL`, not supported.

**Example**

Query:

``` sql
SELECT catalog_name,
       schema_name,
       schema_owner,
       default_character_set_catalog,
       default_character_set_schema,
       default_character_set_name,
       sql_path
FROM information_schema.schemata
WHERE schema_name ilike 'information_schema' 
LIMIT 1 
FORMAT Vertical;
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

- `table_catalog` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
- `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
- `table_name` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `table_type` ([String](../../sql-reference/data-types/string.md)) — Table type. Possible values:
    - `BASE TABLE`
    - `VIEW`
    - `FOREIGN TABLE`
    - `LOCAL TEMPORARY`
    - `SYSTEM VIEW`
- `table_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The total
  number of rows. NULL if it could not be determined.
- `data_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The size of
  the data on-disk. NULL if it could not be determined.
- `table_collation` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The table default collation. Always `utf8mb4_0900_ai_ci`.
- `table_comment` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The comment used when creating the table.

**Example**

Query:

``` sql
SELECT table_catalog, 
       table_schema, 
       table_name, 
       table_type, 
       table_collation, 
       table_comment
FROM INFORMATION_SCHEMA.TABLES
WHERE (table_schema = currentDatabase() OR table_schema = '')
  AND table_name NOT LIKE '%inner%'
LIMIT 1 
FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
table_catalog:   default
table_schema:    default
table_name:      describe_example
table_type:      BASE TABLE
table_collation: utf8mb4_0900_ai_ci
table_comment:   
```

## VIEWS {#views}

Contains columns read from the [system.tables](../../operations/system-tables/tables.md) system table, when the table engine [View](../../engines/table-engines/special/view.md) is used.

Columns:

- `table_catalog` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
- `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the database in which the table is located.
- `table_name` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `view_definition` ([String](../../sql-reference/data-types/string.md)) — `SELECT` query for view.
- `check_option` ([String](../../sql-reference/data-types/string.md)) — `NONE`, no checking.
- `is_updatable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, the view is not updated.
- `is_insertable_into` ([Enum8](../../sql-reference/data-types/enum.md)) — Shows whether the created view is [materialized](../../sql-reference/statements/create/view.md/#materialized-view). Possible values:
    - `NO` — The created view is not materialized.
    - `YES` — The created view is materialized.
- `is_trigger_updatable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, the trigger is not updated.
- `is_trigger_deletable` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, the trigger is not deleted.
- `is_trigger_insertable_into` ([Enum8](../../sql-reference/data-types/enum.md)) — `NO`, no data is inserted into the trigger.

**Example**

Query:

``` sql
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
CREATE MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM system.one;
SELECT table_catalog,
       table_schema,
       table_name,
       view_definition,
       check_option,
       is_updatable,
       is_insertable_into,
       is_trigger_updatable,
       is_trigger_deletable,
       is_trigger_insertable_into
FROM information_schema.views
WHERE table_schema = currentDatabase() 
LIMIT 1
FORMAT Vertical;
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

## KEY_COLUMN_USAGE {#key_column_usage}

Contains columns from the [system.tables](../../operations/system-tables/tables.md) system table which are restricted by constraints.

Columns:

- `constraint_catalog` ([String](../../sql-reference/data-types/string.md)) — Currently unused. Always `def`.
- `constraint_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the schema (database) to which the constraint belongs.
- `constraint_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The name of the constraint.
- `table_catalog` ([String](../../sql-reference/data-types/string.md)) — Currently unused. Always `def`.
- `table_schema` ([String](../../sql-reference/data-types/string.md)) — The name of the schema (database) to which the table belongs.
- `table_name` ([String](../../sql-reference/data-types/string.md)) — The name of the table that has the constraint.
- `column_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The name of the column that has the constraint.
- `ordinal_position` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Currently unused. Always `1`.
- `position_in_unique_constraint` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt32](../../sql-reference/data-types/int-uint.md))) — Currently unused. Always `NULL`.
- `referenced_table_schema` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused. Always NULL.
- `referenced_table_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused. Always NULL.
- `referenced_column_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused. Always NULL.

**Example**

```sql
CREATE TABLE test (i UInt32, s String) ENGINE MergeTree ORDER BY i;
SELECT constraint_catalog,
       constraint_schema,
       constraint_name,
       table_catalog,
       table_schema,
       table_name,
       column_name,
       ordinal_position,
       position_in_unique_constraint,
       referenced_table_schema,
       referenced_table_name,
       referenced_column_name
FROM information_schema.key_column_usage 
WHERE table_name = 'test' 
FORMAT Vertical;
```

Result:

```
Row 1:
──────
constraint_catalog:            def
constraint_schema:             default
constraint_name:               PRIMARY
table_catalog:                 def
table_schema:                  default
table_name:                    test
column_name:                   i
ordinal_position:              1
position_in_unique_constraint: ᴺᵁᴸᴸ
referenced_table_schema:       ᴺᵁᴸᴸ
referenced_table_name:         ᴺᵁᴸᴸ
referenced_column_name:        ᴺᵁᴸᴸ
```

## REFERENTIAL_CONSTRAINTS {#referential_constraints}

Contains information about foreign keys. Currently returns an empty result (no rows) which is just enough to provide compatibility with 3rd party tools like Tableau Online.

Columns:

- `constraint_catalog` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `constraint_schema` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `constraint_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
- `unique_constraint_catalog` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `unique_constraint_schema` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `unique_constraint_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
- `match_option` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `update_rule` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `delete_rule` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `table_name` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `referenced_table_name` ([String](../../sql-reference/data-types/string.md)) — Currently unused.

## STATISTICS {#statistics}

Provides information about table indexes. Currently returns an empty result (no rows) which is just enough to provide compatibility with 3rd party tools like Tableau Online.

Columns:

- `table_catalog` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `table_schema` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `table_name` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `non_unique` ([Int32](../../sql-reference/data-types/int-uint.md)) — Currently unused.
- `index_schema` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `index_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
- `seq_in_index` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Currently unused.
- `column_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
- `collation` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
- `cardinality` ([Nullable](../../sql-reference/data-types/nullable.md)([Int64](../../sql-reference/data-types/int-uint.md))) — Currently unused.
- `sub_part` ([Nullable](../../sql-reference/data-types/nullable.md)([Int64](../../sql-reference/data-types/int-uint.md))) — Currently unused.
- `packed` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
- `nullable` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `index_type` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `comment` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `index_comment` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `is_visible` ([String](../../sql-reference/data-types/string.md)) — Currently unused.
- `expression` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Currently unused.
