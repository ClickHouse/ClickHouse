---
description: 'System table containing the query log records of the current user.'
sidebar_label: 'user_query_log'
sidebar_position: 67
slug: /operations/system-tables/user_query_log
title: 'system.user_query_log'
doc_type: 'reference'
---

# `system.user_query_log` {#system-user-query-log}

Shows the current user their own query log records. It reads the query log table configured by the
`query_log.database` and `query_log.table` server settings (`system.query_log` by default) and returns
only the rows whose initiating user is equal to `currentUser()` (the initiating user is taken from
`initial_user` when it is set, otherwise from `user`).

Unlike the query log table itself, `system.user_query_log` can be read without any grants, so users can
inspect their own queries without being given access to the queries of others.

This is only supported when the query log is stored locally. If `query_log.engine` is configured as
`Distributed` or any other engine that delegates reads to another server, `system.user_query_log` refuses
to read from it and throws an exception, because the required access check cannot be enforced across a
ClickHouse-protocol server boundary. In that case, disable the table with
`query_log.enable_user_query_log = 0`.

The table can be disabled with the `query_log.enable_user_query_log` server setting. If the query log
is not configured, or its table has not been created yet, `system.user_query_log` is empty.

Conditions on the partition and key columns of the query log (`event_date`, `event_time`,
`query_start_time`, `query_id`, `type`, and similar scalar columns) compared with constants are
pushed down to the backing query log table, so ordinary lookups such as the example below keep
partition pruning and do not scan the whole retained log.

:::warning Upgrade compatibility
If a table named `system.user_query_log` was created before upgrading to a ClickHouse version that
provides this table, the server will not start until the existing table is renamed or dropped, or
`query_log.enable_user_query_log` is set to 0.
:::

## Columns {#columns}

The columns are the same as in [`system.query_log`](query_log.md), except that `LowCardinality` wrappers
are removed from the exposed column types.

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
