# parts_columns {#system_tables-parts_columns}

Contains information about parts and columns of [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Each row describes one data part.

Columns:

-   `partition` ([String](../../sql-reference/data-types/string.md)) — The partition name. To learn what a partition is, see the description of the [ALTER](../../sql-reference/statements/alter/index.md#query_language_queries_alter) query.

    Formats:

    -   `YYYYMM` for automatic partitioning by month.
    -   `any_string` when partitioning manually.

-   `name` ([String](../../sql-reference/data-types/string.md)) — Name of the data part.

-   `part_type` ([String](../../sql-reference/data-types/string.md)) — The data part storing format.

    Possible values:

    -   `Wide` — Each column is stored in a separate file in a filesystem.
    -   `Compact` — All columns are stored in one file in a filesystem.

    Data storing format is controlled by the `min_bytes_for_wide_part` and `min_rows_for_wide_part` settings of the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table.

-   `active` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the data part is active. If a data part is active, it’s used in a table. Otherwise, it’s deleted. Inactive data parts remain after merging.

-   `marks` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of marks. To get the approximate number of rows in a data part, multiply `marks` by the index granularity (usually 8192) (this hint does not work for adaptive granularity).

-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows.

-   `bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total size of all the data part files in bytes.

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of the file with marks.

-   `modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The time the directory with the data part was modified. This usually corresponds to the time of data part creation.

-   `remove_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The time when the data part became inactive.

-   `refcount` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` ([Date](../../sql-reference/data-types/date.md)) — The minimum value of the date key in the data part.

-   `max_date` ([Date](../../sql-reference/data-types/date.md)) — The maximum value of the date key in the data part.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — ID of the partition.

-   `min_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The minimum number of data parts that make up the current part after merging.

-   `max_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The maximum number of data parts that make up the current part after merging.

-   `level` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The amount of memory (in bytes) reserved for primary key values.

-   `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database.

-   `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table.

-   `engine` ([String](../../sql-reference/data-types/string.md)) — Name of the table engine without parameters.

-   `disk_name` ([String](../../sql-reference/data-types/string.md)) — Name of a disk that stores the data part.

-   `path` ([String](../../sql-reference/data-types/string.md)) — Absolute path to the folder with data part files.

-   `column` ([String](../../sql-reference/data-types/string.md)) — Name of the column.

-   `type` ([String](../../sql-reference/data-types/string.md)) — Column type.

-   `column_position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Ordinal position of a column in a table starting with 1.

-   `default_kind` ([String](../../sql-reference/data-types/string.md)) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) for the default value, or an empty string if it is not defined.

-   `default_expression` ([String](../../sql-reference/data-types/string.md)) — Expression for the default value, or an empty string if it is not defined.

-   `column_bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total size of the column in bytes.

-   `column_data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total size of compressed data in the column, in bytes.

-   `column_data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total size of the decompressed data in the column, in bytes.

-   `column_marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of the column with marks, in bytes.

-   `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Alias for `bytes_on_disk`.

-   `marks_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Alias for `marks_bytes`.

**Example**

``` sql
SELECT * FROM system.parts_columns LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_2_1
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  2
bytes_on_disk:                         155
data_compressed_bytes:                 56
data_uncompressed_bytes:               4
marks_bytes:                           96
modification_time:                     2020-09-23 10:13:36
remove_time:                           2106-02-07 06:28:15
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
partition_id:                          all
min_block_number:                      1
max_block_number:                      2
level:                                 1
data_version:                          1
primary_key_bytes_in_memory:           2
primary_key_bytes_in_memory_allocated: 64
database:                              default
table:                                 53r93yleapyears
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/53r93yleapyears/all_1_2_1/
column:                                id
type:                                  Int8
column_position:                       1
default_kind:
default_expression:
column_bytes_on_disk:                  76
column_data_compressed_bytes:          28
column_data_uncompressed_bytes:        2
column_marks_bytes:                    48
```

**See Also**

-   [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)

[Original article](https://clickhouse.com/docs/en/operations/system_tables/parts_columns) <!--hide-->
