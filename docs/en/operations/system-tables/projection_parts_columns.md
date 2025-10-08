---
description: 'System table containing information about columns in projection parts for tables of the MergeTree family'
keywords: ['system table', 'projection part columns']
slug: /operations/system-tables/projections-part-columns
title: 'system.projection_parts'
doc_type: 'reference'
---

# system.projection_parts_columns

This table contains information about columns in projection parts for tables of the MergeTree family.

## Columns {#columns}

| Column                                  | Description                                                                                                                              | Type               |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| `partition`                             | The partition name.                                                                                                                      | String             |
| `name`                                  | Name of the data part.                                                                                                                   | String             |
| `part_type`                             | The data part storing format.                                                                                                            | String             |
| `parent_name`                           | The name of the source (parent) data part.                                                                                               | String             |
| `parent_uuid`                           | The UUID of the source (parent) data part.                                                                                               | UUID               |
| `parent_part_type`                      | The source (parent) data part storing format.                                                                                            | String             |
| `active`                                | Flag that indicates whether the data part is active                                                                                      | UInt8              |
| `marks`                                 | The number of marks.                                                                                                                     | UInt64             |
| `rows`                                  | The number of rows.                                                                                                                      | UInt64             |
| `bytes_on_disk`                         | Total size of all the data part files in bytes.                                                                                          | UInt64             |
| `data_compressed_bytes`                 | Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.                | UInt64             |
| `data_uncompressed_bytes`               | Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.              | UInt64             |
| `marks_bytes`                           | The size of the file with marks.                                                                                                         | UInt64             |
| `parent_marks`                          | The number of marks in the source (parent) part.                                                                                         | UInt64             |
| `parent_rows`                           | The number of rows in the source (parent) part.                                                                                          | UInt64             |
| `parent_bytes_on_disk`                  | Total size of all the source (parent) data part files in bytes.                                                                          | UInt64             |
| `parent_data_compressed_bytes`          | Total size of compressed data in the source (parent) data part.                                                                          | UInt64             |
| `parent_data_uncompressed_bytes`        | Total size of uncompressed data in the source (parent) data part.                                                                        | UInt64             |
| `parent_marks_bytes`                    | The size of the file with marks in the source (parent) data part.                                                                        | UInt64             |
| `modification_time`                     | The time the directory with the data part was modified. This usually corresponds to the time of data part creation.                      | DateTime           |
| `remove_time`                           | The time when the data part became inactive.                                                                                             | DateTime           |
| `refcount`                              | The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.      | UInt32             |
| `min_date`                              | The minimum value for the Date column if that is included in the partition key.                                                          | Date               |
| `max_date`                              | The maximum value for the Date column if that is included in the partition key.                                                          | Date               |
| `min_time`                              | The minimum value for the DateTime column if that is included in the partition key.                                                      | DateTime           |
| `max_time`                              | The maximum value for the DateTime column if that is included in the partition key.                                                      | DateTime           |
| `partition_id`                          | ID of the partition.                                                                                                                     | String             |
| `min_block_number`                      | The minimum number of data parts that make up the current part after merging.                                                            | Int64              |
| `max_block_number`                      | The maximum number of data parts that make up the current part after merging.                                                            | Int64              |
| `level`                                 | Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.                      | UInt32             |
| `data_version`                          | Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than data_version). | UInt64             |
| `primary_key_bytes_in_memory`           | The amount of memory (in bytes) used by primary key values.                                                                              | UInt64             |
| `primary_key_bytes_in_memory_allocated` | The amount of memory (in bytes) reserved for primary key values.                                                                         | UInt64             |
| `database`                              | Name of the database.                                                                                                                    | String             |
| `table`                                 | Name of the table.                                                                                                                       | String             |
| `engine`                                | Name of the table engine without parameters.                                                                                             | String             |
| `disk_name`                             | Name of a disk that stores the data part.                                                                                                | String             |
| `path`                                  | Absolute path to the folder with data part files.                                                                                        | String             |
| `column`                                | Name of the column.                                                                                                                      | String             |
| `type`                                  | Column type.                                                                                                                             | String             |
| `column_position`                       | Ordinal position of a column in a table starting with 1.                                                                                 | UInt64             |
| `default_kind`                          | Expression type (DEFAULT, MATERIALIZED, ALIAS) for the default value, or an empty string if it is not defined.                           | String             |
| `default_expression`                    | Expression for the default value, or an empty string if it is not defined.                                                               | String             |
| `column_bytes_on_disk`                  | Total size of the column in bytes.                                                                                                       | UInt64             |
| `column_data_compressed_bytes`          | Total size of compressed data in the column, in bytes.                                                                                   | UInt64             |
| `column_data_uncompressed_bytes`        | Total size of the decompressed data in the column, in bytes.                                                                             | UInt64             |
| `column_marks_bytes`                    | The size of the column with marks, in bytes.                                                                                             | UInt64             |
| `column_modification_time`              | The last time the column was modified.                                                                                                   | Nullable(DateTime) |
| `bytes`                                 | Alias for bytes_on_disk                                                                                                                  | UInt64             |
| `marks_size`                            | Alias for marks_bytes                                                                                                                    | UInt64             |
| `part_name`                             | Alias for name                                                                                                                           | String             |