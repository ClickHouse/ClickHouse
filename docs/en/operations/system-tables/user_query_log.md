---
description: 'System view containing query log rows for the current user.'
sidebar_label: 'user_query_log'
sidebar_position: 67
slug: /operations/system-tables/user_query_log
title: 'system.user_query_log'
doc_type: 'reference'
---

# `system.user_query_log` {#system-user-query-log}

## Description {#description}

The `system.user_query_log` table is a view over the query log table configured by the `query_log.database` and `query_log.table` server configuration parameters. By default, this is `system.query_log`. The view returns only rows where the `user` column is equal to `currentUser()`.

The view is created with `SQL SECURITY DEFINER` and the `default` user as the definer, so a user can read their own query log rows through `system.user_query_log` without being granted direct access to the configured query log table. Users do not need an explicit `SELECT` grant on `system.user_query_log`.

The filter is placed in `PREWHERE` in the view definition.

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
