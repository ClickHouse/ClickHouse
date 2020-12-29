# system.columns {#system-columns}

Contains information about columns in all the tables.

You can use this table to get information similar to the [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table) query, but for multiple tables at once.

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

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/columns) <!--hide-->
