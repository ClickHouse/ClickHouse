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

A maximum number of columns that can be read from a table in a single query. If a query requires reading a greater number of columns, it throws an exception.

## max_temporary_columns {#max-temporary-columns}

A maximum number of temporary columns that must be kept in RAM at the same time when running a query, including constant columns. If there are more temporary columns than this, it throws an exception.

## max_temporary_non_const_columns {#max-temporary-non-const-columns}

The same thing as 'max_temporary_columns', but without counting constant columns.
Note that constant columns are formed fairly often when running a query, but they require approximately zero computing resources.

## max_subquery_depth {#max-subquery-depth}

Maximum nesting depth of subqueries. If subqueries are deeper, an exception is thrown. By default, 100.

## max_pipeline_depth {#max-pipeline-depth}

Maximum pipeline depth. Corresponds to the number of transformations that each data block goes through during query processing. Counted within the limits of a single server. If the pipeline depth is greater, an exception is thrown. By default, 1000.

## max_ast_depth {#max-ast-depth}

Maximum nesting depth of a query syntactic tree. If exceeded, an exception is thrown.
At this time, it isn't checked during parsing, but only after parsing the query. That is, a syntactic tree that is too deep can be created during parsing, but the query will fail. By default, 1000.

## max_ast_elements {#max-ast-elements}

A maximum number of elements in a query syntactic tree. If exceeded, an exception is thrown.
In the same way as the previous setting, it is checked only after parsing the query. By default, 50,000.

## max_rows_in_set {#max-rows-in-set}

A maximum number of rows for a data set in the IN clause created from a subquery.

## max_bytes_in_set {#max-bytes-in-set}

A maximum number of bytes (uncompressed data) used by a set in the IN clause created from a subquery.

## set_overflow_mode {#set-overflow-mode}

What to do when the amount of data exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_rows_in_distinct {#max-rows-in-distinct}

A maximum number of different rows when using DISTINCT.

## max_bytes_in_distinct {#max-bytes-in-distinct}

A maximum number of bytes used by a hash table when using DISTINCT.

## distinct_overflow_mode {#distinct-overflow-mode}

What to do when the amount of data exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_rows_to_transfer {#max-rows-to-transfer}

A maximum number of rows that can be passed to a remote server or saved in a temporary table when using GLOBAL IN.

## max_bytes_to_transfer {#max-bytes-to-transfer}

A maximum number of bytes (uncompressed data) that can be passed to a remote server or saved in a temporary table when using GLOBAL IN.

## transfer_overflow_mode {#transfer-overflow-mode}

What to do when the amount of data exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_rows_in_join {#settings-max_rows_in_join}

Limits the number of rows in the hash table that is used when joining tables.

This settings applies to [SELECT ... JOIN](/sql-reference/statements/select/join) operations and the [Join](../../engines/table-engines/special/join.md) table engine.

If a query contains multiple joins, ClickHouse checks this setting for every intermediate result.

ClickHouse can proceed with different actions when the limit is reached. Use the [join_overflow_mode](#settings-join_overflow_mode) setting to choose the action.

Possible values:

- Positive integer.
- 0 — Unlimited number of rows.

Default value: 0.

## max_bytes_in_join {#settings-max_bytes_in_join}

Limits the size in bytes of the hash table used when joining tables.

This setting applies to [SELECT ... JOIN](/sql-reference/statements/select/join) operations and [Join table engine](../../engines/table-engines/special/join.md).

If the query contains joins, ClickHouse checks this setting for every intermediate result.

ClickHouse can proceed with different actions when the limit is reached. Use [join_overflow_mode](#settings-join_overflow_mode) settings to choose the action.

Possible values:

- Positive integer.
- 0 — Memory control is disabled.

Default value: 0.

## join_overflow_mode {#settings-join_overflow_mode}

Defines what action ClickHouse performs when any of the following join limits is reached:

- [max_bytes_in_join](#settings-max_bytes_in_join)
- [max_rows_in_join](#settings-max_rows_in_join)

Possible values:

- `THROW` — ClickHouse throws an exception and breaks operation.
- `BREAK` — ClickHouse breaks operation and does not throw an exception.

Default value: `THROW`.

**See Also**

- [JOIN clause](/sql-reference/statements/select/join)
- [Join table engine](../../engines/table-engines/special/join.md)

## max_partitions_per_insert_block {#settings-max_partitions_per_insert_block}

Limits the maximum number of partitions in a single inserted block.

- Positive integer.
- 0 — Unlimited number of partitions.

Default value: 100.

**Details**

When inserting data, ClickHouse calculates the number of partitions in the inserted block. If the number of partitions is more than `max_partitions_per_insert_block`, ClickHouse either logs a warning or throws an exception based on `throw_on_max_partitions_per_insert_block`. Exceptions have the following text:

> "Too many partitions for a single INSERT block (`partitions_count` partitions, limit is " + toString(max_partitions) + "). The limit is controlled by the 'max_partitions_per_insert_block' setting. A large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc)."

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
