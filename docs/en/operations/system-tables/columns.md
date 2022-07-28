# system.columns {#system-columns}

Contains information about columns in all the tables.

You can use this table to get information similar to the [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table) query, but for multiple tables at once.

Columns from [temporary tables](../../sql-reference/statements/create/table.md#temporary-tables) are visible in the `system.columns` only in those session where they have been created. They are shown with the empty `database` field. 

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
-   `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
-   `name` ([String](../../sql-reference/data-types/string.md)) — Column name.
-   `type` ([String](../../sql-reference/data-types/string.md)) — Column type.
-   `position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Ordinal position of a column in a table starting with 1.
-   `default_kind` ([String](../../sql-reference/data-types/string.md)) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) for the default value, or an empty string if it is not defined.
-   `default_expression` ([String](../../sql-reference/data-types/string.md)) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of decompressed data, in bytes.
-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of marks, in bytes.
-   `comment` ([String](../../sql-reference/data-types/string.md)) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the column is in the sampling key expression.
-   `compression_codec` ([String](../../sql-reference/data-types/string.md)) — Compression codec name.

**Example**

```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                system
table:                   aggregate_function_combinators
name:                    name
type:                    String
default_kind:            
default_expression:      
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:                 
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:       

Row 2:
──────
database:                system
table:                   aggregate_function_combinators
name:                    is_internal
type:                    UInt8
default_kind:            
default_expression:      
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:                 
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:       
```

The `system.columns` table contains the following columns (the column type is shown in brackets):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) for the default value, or an empty string if it is not defined.
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/columns) <!--hide-->
