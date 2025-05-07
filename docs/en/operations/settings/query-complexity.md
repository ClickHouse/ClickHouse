---
description: 'Settings which restrict query complexity.'
sidebar_label: 'Restrictions on Query Complexity'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: 'Restrictions on Query Complexity'
---

# Restrictions on Query Complexity

## Overview {#overview}

As part of the [settings](/operations/settings/overview), ClickHouse offers
the ability to place restrictions on query complexity. This helps protect against
potentially resource-intensive queries, ensuring safer and more predictable 
execution, particularly when using the user interface.

Almost all the restrictions only apply to `SELECT` queries, and for distributed 
query processing, restrictions are applied on each server separately.

ClickHouse generally checks the restrictions only after data parts have been 
fully processed, rather than checking the restrictions for each row. This can
result in a situation where restrictions are violated while the part is being
processed.

## `overflow_mode` settings {#overflow_mode_setting}

Most restrictions also have an `overflow_mode` setting, which defines what happens
when the limit is exceeded, and can take one of two values:
- `throw`: throw an exception (default).
- `break`: stop executing the query and return the partial result, as if the 
           source data ran out.

## `group_by_overflow_mode` settings {#group_by_overflow_mode_settings}

The `group_by_overflow_mode` setting also has
the value `any`:
- `any` : continue aggregation for the keys that got into the set, but do not 
          add new keys to the set.

## List of settings {#relevant-settings}

The following settings are used for applying restrictions on query complexity.

:::note
Restrictions on the "maximum amount of something" can take a value of `0`,
which means that it is "unrestricted".
:::

| Setting                                                                                                                | Short description                                                                                                                                               |
|------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`max_memory_usage`](/operations/settings/settings#max_memory_usage)                                                   | The maximum amount of RAM to use for running a query on a single server.                                                                                        |
| [`max_memory_usage_for_user`](/operations/settings/settings#max_memory_usage_for_user)                                 | The maximum amount of RAM to use for running a user's queries on a single server.                                                                               |
| [`max_rows_to_read`](/operations/settings/settings#max_rows_to_read)                                                   | The maximum number of rows that can be read from a table when running a query.                                                                                  |
| [`max_bytes_to_read`](/operations/settings/settings#max_bytes_to_read)                                                 | The maximum number of bytes (of uncompressed data) that can be read from a table when running a query.                                                          |
| [`read_overflow_mode_leaf`](/operations/settings/settings#read_overflow_mode_leaf)                                     | Sets what happens when the volume of data read exceeds one of the leaf limits                                                                                   |
| [`max_rows_to_read_leaf`](/operations/settings/settings#max_rows_to_read_leaf)                                         | The maximum number of rows that can be read from a local table on a leaf node when running a distributed query                                                  |
| [`max_bytes_to_read_leaf`](/operations/settings/settings#max_bytes_to_read_leaf)                                       | The maximum number of bytes (of uncompressed data) that can be read from a local table on a leaf node when running a distributed query.                         |
| [`read_overflow_mode_leaf`](/docs/operations/settings/settings#read_overflow_mode_leaf)                                | Sets what happens when the volume of data read exceeds one of the leaf limits.                                                                                  |
| [`max_rows_to_group_by`](/operations/settings/settings#max_rows_to_group_by)                                           | The maximum number of unique keys received from aggregation.                                                                                                    |
| [`group_by_overflow_mode`](/operations/settings/settings#group_by_overflow_mode)                                       | Sets what happens when the number of unique keys for aggregation exceeds the limit                                                                              |
| [`max_bytes_before_external_group_by`](/operations/settings/settings#max_bytes_before_external_group_by)               | Enables or disables execution of `GROUP BY` clauses in external memory.                                                                                         |
| [`max_bytes_ratio_before_external_group_by`](/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | The ratio of available memory that is allowed for `GROUP BY`. Once reached, external memory is used for aggregation.                                            |
| [`max_bytes_before_external_sort`](/operations/settings/settings#max_bytes_before_external_sort)                       | Enables or disables execution of `ORDER BY` clauses in external memory.                                                                                         |
| [`max_bytes_ratio_before_external_sort`](/operations/settings/settings#max_bytes_ratio_before_external_sort)           | The ratio of available memory that is allowed for `ORDER BY`. Once reached, external sort is used.                                                              |
| [`max_rows_to_sort`](/operations/settings/settings#max_rows_to_sort)                                                   | The maximum number of rows before sorting. Allows limiting memory consumption when sorting.                                                                     |
| [`max_bytes_to_sort`](/operations/settings/settings#max_rows_to_sort)                                                  | The maximum number of bytes before sorting.                                                                                                                     |
| [`sort_overflow_mode`](/operations/settings/settings#sort_overflow_mode)                                               | Sets what happens if the number of rows received before sorting exceeds one of the limits.                                                                      |
| [`max_result_rows`](/operations/settings/settings#max_result_rows)                                                     | Limits the number of rows in the result.                                                                                                                        |
| [`max_result_bytes`](/operations/settings/settings#max_result_bytes)                                                   | Limits the result size in bytes (uncompressed)                                                                                                                  |
| [`result_overflow_mode`](/operations/settings/settings#result_overflow_mode)                                           | Sets what to do if the volume of the result exceeds one of the limits.                                                                                          |
| [`max_execution_time`](/operations/settings/settings#max_execution_time)                                               | The maximum query execution time in seconds.                                                                                                                    |
| [`timeout_overflow_mode`](/operations/settings/settings#timeout_overflow_mode)                                         | Sets what to do if the query is run longer than the `max_execution_time` or the estimated running time is longer than `max_estimated_execution_time`.           |
| [`max_execution_time_leaf`](/operations/settings/settings#max_execution_time_leaf)                                     | Similar semantically to `max_execution_time` but only applied on leaf nodes for distributed or remote queries.                                                  |
| [`timeout_overflow_mode_leaf`](/operations/settings/settings#timeout_overflow_mode_leaf)                               | Sets what happens when the query in leaf node run longer than `max_execution_time_leaf`.                                                                        |
| [`min_execution_speed`](/operations/settings/settings#min_execution_speed)                                             | Minimal execution speed in rows per second.                                                                                                                     |
| [`min_execution_speed_bytes`](/operations/settings/settings#min_execution_speed_bytes)                                 | The minimum number of execution bytes per second.                                                                                                               |
| [`max_execution_speed`](/operations/settings/settings#max_execution_speed)                                             | The maximum number of execution rows per second.                                                                                                                |
| [`max_execution_speed_bytes`](/operations/settings/settings#max_execution_speed_bytes)                                 | The maximum number of execution bytes per second.                                                                                                               |
| [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed)     | Checks that execution speed is not too slow (no less than `min_execution_speed`), after the specified time in seconds has expired.                              |
| [`max_estimated_execution_time`](/operations/settings/settings#max_estimated_execution_time)                           | Maximum query estimate execution time in seconds.                                                                                                               |
| [`max_columns_to_read`](/operations/settings/settings#max_columns_to_read)                                             | The maximum number of columns that can be read from a table in a single query.                                                                                  |
| [`max_temporary_columns`](/operations/settings/settings#max_temporary_columns)                                         | The maximum number of temporary columns that must be kept in RAM simultaneously when running a query, including constant columns.                               |
| [`max_temporary_non_const_columns`](/operations/settings/settings#max_temporary_non_const_columns)                     | The maximum number of temporary columns that must be kept in RAM simultaneously when running a query, but without counting constant columns.                    |
| [`max_subquery_depth`](/operations/settings/settings#max_subquery_depth)                                               | Sets what happens if a query has more than the specified number of nested subqueries.                                                                           |
| [`max_ast_depth`](/operations/settings/settings#max_ast_depth)                                                         | The maximum nesting depth of a query syntactic tree.                                                                                                            |
| [`max_ast_elements`](/operations/settings/settings#max_ast_elements)                                                   | The maximum number of elements in a query syntactic tree.                                                                                                       |
| [`max_rows_in_set`](/operations/settings/settings#max_rows_in_set)                                                     | The maximum number of rows for a data set in the IN clause created from a subquery.                                                                             |
| [`max_bytes_in_set`](/operations/settings/settings#max_bytes_in_set)                                                   | The maximum number of bytes (of uncompressed data) used by a set in the IN clause created from a subquery.                                                      |
| [`set_overflow_mode`](/operations/settings/settings#max_bytes_in_set)                                                  | Sets what happens when the amount of data exceeds one of the limits.                                                                                            |
| [`max_rows_in_distinct`](/operations/settings/settings#max_rows_in_distinct)                                           | The maximum number of different rows when using DISTINCT.                                                                                                       |
| [`max_bytes_in_distinct`](/operations/settings/settings#max_bytes_in_distinct)                                         | The maximum number of bytes of the state (in uncompressed bytes) in memory, which is used by a hash table when using DISTINCT.                                  |
| [`distinct_overflow_mode`](/operations/settings/settings#distinct_overflow_mode)                                       | Sets what happens when the amount of data exceeds one of the limits.                                                                                            |
| [`max_rows_to_transfer`](/operations/settings/settings#max_rows_to_transfer)                                           | Maximum size (in rows) that can be passed to a remote server or saved in a temporary table when the GLOBAL IN/JOIN section is executed.                         |
| [`max_bytes_to_transfer`](/operations/settings/settings#max_bytes_to_transfer)                                         | The maximum number of bytes (uncompressed data) that can be passed to a remote server or saved in a temporary table when the GLOBAL IN/JOIN section is executed. |
| [`transfer_overflow_mode`](/operations/settings/settings#transfer_overflow_mode)                                       | Sets what happens when the amount of data exceeds one of the limits.                                                                                            |
| [`max_rows_in_join`](/operations/settings/settings#max_rows_in_join)                                                   | Limits the number of rows in the hash table that is used when joining tables.                                                                                   |
| [`max_bytes_in_join`](/operations/settings/settings#max_bytes_in_join)                                                 | The maximum size in number of bytes of the hash table used when joining tables.                                                                                 |
| [`join_overflow_mode`](/operations/settings/settings#join_overflow_mode)                                               | Defines what action ClickHouse performs when any of the following join limits is reached.                                                                       |
| [`max_partitions_per_insert_block`](/operations/settings/settings#max_partitions_per_insert_block)                     | Limits the maximum number of partitions in a single inserted block and an exception is thrown if the block contains too many partitions.                        |
| [`throw_on_max_partitions_per_insert_block`](/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | Allows you to control the behaviour when `max_partitions_per_insert_block` is reached.                                                                          |
| [`max_temporary_data_on_disk_size_for_user`](/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries.                                              |
| [`max_temporary_data_on_disk_size_for_query`](/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries.                                                   |
| [`max_sessions_for_user`](/operations/settings/settings#max_sessions_for_user)                                         | Maximum number of simultaneous sessions per authenticated user to the ClickHouse server.                                                                        |
| [`max_partitions_to_read`](/operations/settings/settings#max_partitions_to_read)                                       | Limits the maximum number of partitions that can be accessed in a single query.                                                                                 |

## Obsolete settings {#obsolete-settings}

:::note
The following settings are obsolete
:::

### max_pipeline_depth {#max-pipeline-depth}

Maximum pipeline depth. It Corresponds to the number of transformations that each 
data block goes through during query processing. Counted within the limits of a 
single server. If the pipeline depth is greater, an exception is thrown.
