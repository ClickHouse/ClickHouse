---
description: 'System view containing query log rows for the current user.'
sidebar_label: 'user_query_log'
sidebar_position: 67
slug: /operations/system-tables/user_query_log
title: 'system.user_query_log'
doc_type: 'reference'
---

# `system.user_query_log` {#system-user-query-log}

:::warning Upgrade compatibility

The name `system.user_query_log` is reserved for this system view. Before upgrading from a ClickHouse version that does not provide this view, check whether an existing table uses this name:

```sql
SELECT database, name, engine
FROM system.tables
WHERE database = 'system' AND name = 'user_query_log';
```

If the query returns a table, rename or drop it using the old ClickHouse version before upgrading. Otherwise, the new version will refuse to start because it cannot safely replace the existing table. Renaming the table preserves its data:

```sql
RENAME TABLE system.user_query_log TO system.user_query_log_legacy;
```

Also update the `query_log.table` server configuration parameter if it is set to `user_query_log` while `query_log.database` is set to `system`. For example, after the rename above, set `query_log.table` to `user_query_log_legacy` if the renamed table should remain the query log destination.

:::

## Description {#description}

The `system.user_query_log` table is a view over the query log table configured by the `query_log.database` and `query_log.table` server configuration parameters. By default, this is `system.query_log`. The view returns only rows where the initiating user is equal to the result of `currentUser()`. For distributed query log rows, the initiating user is stored in `initial_user`; otherwise, `user` is used.

The view is created with `SQL SECURITY NONE`, so its inner query uses internal access to the configured query log table instead of a `DEFINER` user. This lets a user read their own query log rows through `system.user_query_log` without being granted direct access to the configured query log table. Users do not need an explicit `SELECT` grant on `system.user_query_log`.

You can disable the view with the `query_log.enable_user_query_log` server configuration parameter.

In `clickhouse-local`, `system.user_query_log` is available only when `query_log` is explicitly configured, because `clickhouse-local` does not initialize system logs by default.

## Columns {#columns}

The columns match the configured query log table, which uses the same structure as [`system.query_log`](query_log.md), except `LowCardinality` wrappers are removed from exposed column types.

## Example {#example}

```sql
SELECT
    query_start_time,
    query_duration_ms,
    query
FROM system.user_query_log
ORDER BY query_start_time DESC
LIMIT 10;
```
