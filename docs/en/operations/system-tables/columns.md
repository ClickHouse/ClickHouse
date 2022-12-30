# columns

Contains information about columns in all the tables.

You can use this table to get information similar to the [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table) query, but for multiple tables at once.

Columns from [temporary tables](../../sql-reference/statements/create/table.md#temporary-tables) are visible in the `system.columns` only in those session where they have been created. They are shown with the empty `database` field.

The `system.columns` table contains the following columns (the column type is shown in brackets):

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
-   `character_octet_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum length in bytes for binary data, character data, or text data and images. In ClickHouse makes sense only for `FixedString` data type. Otherwise, the `NULL` value is returned.
-   `numeric_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Accuracy of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse it is bitness for integer types and decimal precision for `Decimal` types. Otherwise, the `NULL` value is returned.
-   `numeric_precision_radix` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The base of the number system is the accuracy of approximate numeric data, exact numeric data, integer data or monetary data. In ClickHouse it's 2 for integer types and 10 for `Decimal` types. Otherwise, the `NULL` value is returned.
-   `numeric_scale` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The scale of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse makes sense only for `Decimal` types. Otherwise, the `NULL` value is returned.
-   `datetime_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Decimal precision of `DateTime64` data type. For other data types, the `NULL` value is returned.

**Example**

```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_catalog
type:                    String
position:                1
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
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ

Row 2:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_schema
type:                    String
position:                2
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
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/columns) <!--hide-->
