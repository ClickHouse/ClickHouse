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

## overflow_mode settings {#overflow_mode_setting}

Most restrictions also have an `overflow_mode` setting, which defines what happens
when the limit is exceeded, and take one of two values:
- `throw`: throw an exception (default).
- `break`: stop executing the query and return the partial result, as if the 
           source data ran out.

## group_by_overflow_mode settings {#group_by_overflow_mode_settings}

Restrictions on aggregation (`group_by_overflow_mode`) also have the value `any`:
- `any` : continue aggregation for the keys that got into the set, but do not 
          add new keys to the set.

## Relevant settings {#relevant-settings}

The following settings apply to query complexity.

:::note
Restrictions on the "maximum amount of something" can take a value of `0`,
which means that it is "unrestricted".
:::

## max_memory_usage {#settings_max_memory_usage}

See [`max_memory_usage`](/operations/settings/settings#max_memory_usage)

## max_memory_usage_for_user {#max-memory-usage-for-user}

See [`max_memory_usage_for_user`](/operations/settings/settings#max_memory_usage_for_user)

## max_rows_to_read {#max-rows-to-read}

See [`max_rows_to_read`](/operations/settings/settings#max_rows_to_read)

## max_bytes_to_read {#max-bytes-to-read}

See [`max_bytes_to_read`](/operations/settings/settings#max_bytes_to_read)

## read_overflow_mode {#read-overflow-mode}

See [`read_overflow_mode_leaf`](/operations/settings/settings#read_overflow_mode_leaf)

## max_rows_to_read_leaf {#max-rows-to-read-leaf}

See [`max_rows_to_read_leaf`](/operations/settings/settings#max_rows_to_read_leaf)

## max_bytes_to_read_leaf {#max-bytes-to-read-leaf}

See [`max_bytes_to_read_leaf`](/operations/settings/settings#max_bytes_to_read_leaf)

## read_overflow_mode_leaf {#read-overflow-mode-leaf}

See [`read_overflow_mode_leaf`](/docs/operations/settings/settings#read_overflow_mode_leaf)

## max_rows_to_group_by {#settings-max-rows-to-group-by}

See [`max_rows_to_group_by`](/operations/settings/settings#max_rows_to_group_by)

## group_by_overflow_mode {#group-by-overflow-mode}

See [`group_by_overflow_mode`]

## max_bytes_before_external_group_by {#settings-max_bytes_before_external_group_by}

See [`max_bytes_before_external_group_by`](/operations/settings/settings#max_bytes_before_external_group_by)

## max_bytes_ratio_before_external_group_by {#settings-max_bytes_ratio_before_external_group_by}

See [`max_bytes_ratio_before_external_group_by`](/operations/settings/settings#max_bytes_ratio_before_external_group_by)

## max_bytes_before_external_sort {#settings-max_bytes_before_external_sort}

See [`max_bytes_before_external_sort`](/operations/settings/settings#max_bytes_before_external_sort)

## max_bytes_ratio_before_external_sort {#settings-max_bytes_ratio_before_external_sort}

See [`max_bytes_ratio_before_external_sort`](/operations/settings/settings#max_bytes_ratio_before_external_sort)

## max_rows_to_sort {#max-rows-to-sort}

See [`max_rows_to_sort`](/operations/settings/settings#max_rows_to_sort)

## max_bytes_to_sort {#max-bytes-to-sort}

See [`max_bytes_to_sort`](/operations/settings/settings#max_rows_to_sort)

## sort_overflow_mode {#sort-overflow-mode}

See [`sort_overflow_mode`](/operations/settings/settings#sort_overflow_mode)

## max_result_rows {#setting-max_result_rows}

See [`max_result_rows`](/operations/settings/settings#max_result_rows)

## max_result_bytes {#max-result-bytes}

See [`max_result_bytes`](/operations/settings/settings#max_result_bytes)

## result_overflow_mode {#result-overflow-mode}

See [`result_overflow_mode`](/operations/settings/settings#result_overflow_mode)

## max_execution_time {#max-execution-time}

See [`max_execution_time`](/operations/settings/settings#max_execution_time)

## timeout_overflow_mode {#timeout-overflow-mode}

See [`timeout_overflow_mode`](/operations/settings/settings#timeout_overflow_mode)

## max_execution_time_leaf {#max_execution_time_leaf}

See [`max_execution_time_leaf`](/operations/settings/settings#max_execution_time_leaf)

## timeout_overflow_mode_leaf {#timeout_overflow_mode_leaf}

See [`timeout_overflow_mode_leaf`](/operations/settings/settings#timeout_overflow_mode_leaf)

## min_execution_speed {#min-execution-speed}

See [`min_execution_speed`](/operations/settings/settings#min_execution_speed)

## min_execution_speed_bytes {#min-execution-speed-bytes}

See [`min_execution_speed_bytes`](/operations/settings/settings#min_execution_speed_bytes)

## max_execution_speed {#max-execution-speed}

See [`max_execution_speed`](/operations/settings/settings#max_execution_speed)

## max_execution_speed_bytes {#max-execution-speed-bytes}

See [`max_execution_speed_bytes`](/operations/settings/settings#max_execution_speed_bytes)

## timeout_before_checking_execution_speed {#timeout-before-checking-execution-speed}

See [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed)

## max_estimated_execution_time {#max_estimated_execution_time}

See [`max_estimated_execution_time`](/operations/settings/settings#max_estimated_execution_time)

## max_columns_to_read {#max-columns-to-read}

See [`max_columns_to_read`](/operations/settings/settings#max_columns_to_read)

## max_temporary_columns {#max-temporary-columns}

See [`max_temporary_columns`](/operations/settings/settings#max_temporary_columns)

## max_temporary_non_const_columns {#max-temporary-non-const-columns}

See [`max_temporary_non_const_columns`](/operations/settings/settings#max_temporary_non_const_columns)

## max_subquery_depth {#max-subquery-depth}

See [`max_subquery_depth`](/operations/settings/settings#max_subquery_depth)

## max_pipeline_depth {#max-pipeline-depth}

Maximum pipeline depth. It Corresponds to the number of transformations that each 
data block goes through during query processing. Counted within the limits of a 
single server. If the pipeline depth is greater, an exception is thrown.

:::note
This is an obsolete setting
:::

## max_ast_depth {#max-ast-depth}

See [`max_ast_depth`](/operations/settings/settings#max_ast_depth)

## max_ast_elements {#max-ast-elements}

See [`max_ast_elements`](/operations/settings/settings#max_ast_elements)

## max_rows_in_set {#max-rows-in-set}

See [`max_rows_in_set`](/operations/settings/settings#max_rows_in_set)

## max_bytes_in_set {#max-bytes-in-set}

See [`max_bytes_in_set`](/operations/settings/settings#max_bytes_in_set)

## set_overflow_mode {#set-overflow-mode}

See [`set_overflow_mode`](/operations/settings/settings#max_bytes_in_set)

## max_rows_in_distinct {#max-rows-in-distinct}

See [`max_rows_in_distinct`](/operations/settings/settings#max_rows_in_distinct)

## max_bytes_in_distinct {#max-bytes-in-distinct}

See [`max_bytes_in_distinct`](/operations/settings/settings#max_bytes_in_distinct)

## distinct_overflow_mode {#distinct-overflow-mode}

See [`distinct_overflow_mode`](/operations/settings/settings#distinct_overflow_mode)

## max_rows_to_transfer {#max-rows-to-transfer}

See [`max_rows_to_transfer`](/operations/settings/settings#max_rows_to_transfer)

## max_bytes_to_transfer {#max-bytes-to-transfer}

See [`max_bytes_to_transfer`](/operations/settings/settings#max_bytes_to_transfer)

## transfer_overflow_mode {#transfer-overflow-mode}

See [`transfer_overflow_mode`](/operations/settings/settings#transfer_overflow_mode)

## max_rows_in_join {#settings-max_rows_in_join}

See [`max_rows_in_join`](/operations/settings/settings#max_rows_in_join)

## max_bytes_in_join {#settings-max_bytes_in_join}

See [`max_bytes_in_join`](/operations/settings/settings#max_bytes_in_join)

## join_overflow_mode {#settings-join_overflow_mode}

See [`join_overflow_mode`](/operations/settings/settings#join_overflow_mode)

## max_partitions_per_insert_block {#settings-max_partitions_per_insert_block}

See [`max_partitions_per_insert_block`](/operations/settings/settings#max_partitions_per_insert_block)

## throw_on_max_partitions_per_insert_block {#settings-throw_on_max_partition_per_insert_block}

Allows you to control behaviour when `max_partitions_per_insert_block` is reached.

- `true`  - When an insert block reaches `max_partitions_per_insert_block`, an exception is raised.
- `false` - Logs a warning when `max_partitions_per_insert_block` is reached.

Default value: `true`

## max_temporary_data_on_disk_size_for_user {#settings_max_temporary_data_on_disk_size_for_user}

The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries.
Zero means unlimited.

Default value: 0.


## max_temporary_data_on_disk_size_for_query {#settings_max_temporary_data_on_disk_size_for_query}

The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries.
Zero means unlimited.

Default value: 0.

## max_sessions_for_user {#max-sessions-per-user}

Maximum number of simultaneous sessions per authenticated user to the ClickHouse server.

Example:

```xml
<profiles>
    <single_session_profile>
        <max_sessions_for_user>1</max_sessions_for_user>
    </single_session_profile>
    <two_sessions_profile>
        <max_sessions_for_user>2</max_sessions_for_user>
    </two_sessions_profile>
    <unlimited_sessions_profile>
        <max_sessions_for_user>0</max_sessions_for_user>
    </unlimited_sessions_profile>
</profiles>
<users>
     <!-- User Alice can connect to a ClickHouse server no more than once at a time. -->
    <Alice>
        <profile>single_session_user</profile>
    </Alice>
    <!-- User Bob can use 2 simultaneous sessions. -->
    <Bob>
        <profile>two_sessions_profile</profile>
    </Bob>
    <!-- User Charles can use arbitrarily many of simultaneous sessions. -->
    <Charles>
       <profile>unlimited_sessions_profile</profile>
    </Charles>
</users>
```

Default value: 0 (Infinite count of simultaneous sessions).

## max_partitions_to_read {#max-partitions-to-read}

Limits the maximum number of partitions that can be accessed in one query.

The setting value specified when the table is created can be overridden via query-level setting.

Possible values:

- Any positive integer.

Default value: -1 (unlimited).

You can also specify a MergeTree setting [max_partitions_to_read](merge-tree-settings#max-partitions-to-read) in tables' setting.
