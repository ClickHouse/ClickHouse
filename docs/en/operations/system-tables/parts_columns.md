---
description: 'System table containing information about parts and columns of MergeTree
  tables.'
keywords: ['system table', 'parts_columns']
slug: /operations/system-tables/parts_columns
title: 'system.parts_columns'
doc_type: 'reference'
---

# system.parts_columns

Contains information about parts and columns of [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.
Each row describes one data part.

| Column | Type | Description |
|--------|------|-------------|
| `partition` | String | The partition name. Formats: `YYYYMM` for automatic partitioning by month, or `any_string` when partitioning manually. |
| `name` | String | Name of the data part. |
| `part_type` | String | The data part storing format. Values: `Wide` (each column in separate file) or `Compact` (all columns in one file). Controlled by `min_bytes_for_wide_part` and `min_rows_for_wide_part` settings. |
| `active` | UInt8 | Flag indicating whether the data part is active. Active parts are used in the table; inactive parts are deleted or remain after merging. |
| `marks` | UInt64 | The number of marks. Multiply by index granularity (usually 8192) to get approximate row count. |
| `rows` | UInt64 | The number of rows. |
| `bytes_on_disk` | UInt64 | Total size of all the data part files in bytes. |
| `data_compressed_bytes` | UInt64 | Total size of compressed data in the data part (excludes auxiliary files like marks). |
| `data_uncompressed_bytes` | UInt64 | Total size of uncompressed data in the data part (excludes auxiliary files like marks). |
| `marks_bytes` | UInt64 | The size of the file with marks. |
| `modification_time` | DateTime | The time the directory with the data part was modified (usually corresponds to creation time). |
| `remove_time` | DateTime | The time when the data part became inactive. |
| `refcount` | UInt32 | The number of places where the data part is used. Value > 2 indicates use in queries or merges. |
| `min_date` | Date | The minimum value of the date key in the data part. |
| `max_date` | Date | The maximum value of the date key in the data part. |
| `partition_id` | String | ID of the partition. |
| `min_block_number` | UInt64 | The minimum number of data parts that make up the current part after merging. |
| `max_block_number` | UInt64 | The maximum number of data parts that make up the current part after merging. |
| `level` | UInt32 | Depth of the merge tree. Zero means created by insert, not by merging. |
| `data_version` | UInt64 | Number used to determine which mutations should be applied (mutations with version higher than `data_version`). |
| `primary_key_bytes_in_memory` | UInt64 | The amount of memory (in bytes) used by primary key values. |
| `primary_key_bytes_in_memory_allocated` | UInt64 | The amount of memory (in bytes) reserved for primary key values. |
| `database` | String | Name of the database. |
| `table` | String | Name of the table. |
| `engine` | String | Name of the table engine without parameters. |
| `disk_name` | String | Name of a disk that stores the data part. |
| `path` | String | Absolute path to the folder with data part files. |
| `column` | String | Name of the column. |
| `type` | String | Column type. |
| `statistics` | String | Statistics created for the column. |
| `estimates.min` | String | Estimated minimum value of the column. |
| `estimates.max` | String | Estimated maximum value of the column. |
| `estimates.cardinality` | String | Estimated cardinality of the column. |
| `column_position` | UInt64 | Ordinal position of a column in a table starting with 1. |
| `default_kind` | String | Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) for the default value, or empty string if not defined. |
| `default_expression` | String | Expression for the default value, or empty string if not defined. |
| `column_bytes_on_disk` | UInt64 | Total size of the column in bytes. |
| `column_data_compressed_bytes` | UInt64 | Total size of compressed data in the column, in bytes. Note: this is not calculated for compact parts. |
| `column_data_uncompressed_bytes` | UInt64 | Total size of the decompressed data in the column, in bytes. Note: this is not calculated for compact parts. |
| `column_marks_bytes` | UInt64 | The size of the column with marks, in bytes. |
| `bytes` | UInt64 | Alias for `bytes_on_disk`. |
| `marks_size` | UInt64 | Alias for `marks_bytes`. |

**Example**

```sql
SELECT * FROM system.parts_columns LIMIT 1 FORMAT Vertical;
```

```text
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

- [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)
- [Calculating the number and size of compact and wide parts](/knowledgebase/count-parts-by-type)
