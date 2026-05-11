---
description: 'System table with one row per named cached scalar defined via CREATE NAMED SCALAR.'
keywords: ['system table', 'named_scalars']
slug: /operations/system-tables/named_scalars
title: 'system.named_scalars'
doc_type: 'reference'
---

One row per named cached scalar defined via
[`CREATE NAMED SCALAR`](/sql-reference/statements/create/named-scalar).
Rows are visible to users with either the `getNamedScalar` function-execute
grant or the `SHOW_NAMED_SCALARS` grant. The `getNamedScalar` grant exposes
value-tier columns such as the name, current value, type, and freshness state.
The `SHOW_NAMED_SCALARS` grant additionally exposes operator-tier columns such
as the definition, definer, last error, and in-flight refresh state.

## Columns

Columns marked **value-tier** are populated for users with the `getNamedScalar`
function-execute grant. Columns marked **operator-tier** additionally require
`SHOW_NAMED_SCALARS` and are `NULL` when only the value-tier grant is present.

| Column | Type | Tier | Description |
|---|---|---|---|
| `name` | String | value | Scalar name. |
| `kind` | Enum8(`'local'`=0, `'shared'`=1) | value | Cache kind: `'local'` (per-server) or `'shared'` (Keeper-backed). |
| `value` | Nullable(String) | value | Current value as string; NULL if no value has been produced yet. |
| `loading_start_time` | DateTime | value | Time the definition was loaded into memory. |
| `last_refresh_time` | Nullable(DateTime) | value | Time of the last refresh attempt (NULL if never attempted). |
| `next_refresh_time` | Nullable(DateTime) | value | Next scheduled refresh time; NULL for static scalars. |
| `last_success_time` | Nullable(DateTime) | value | Time of the last successful refresh. |
| `refresh_interval` | Nullable(UInt64) | value | Refresh period in seconds; NULL if no `REFRESH` clause. |
| `type` | Nullable(String) | value | ClickHouse type of the current value. |
| `has_value` | UInt8 | value | 1 if a last-good value exists; 0 otherwise. |
| `current_value_is_valid` | UInt8 | value | 1 if the most recent refresh succeeded; 0 if it threw. |
| `last_refresh_hostname` | Nullable(String) | operator | Host that performed the last refresh (rotates across replicas for `shared`). |
| `definer` | Nullable(String) | operator | User whose privileges are used to evaluate the scalar definition. |
| `expression` | Nullable(String) | operator | The scalar's source query. |
| `exception` | Nullable(String) | operator | Last refresh error, prefixed with `[ERROR_CODE_NAME]:`. |
| `refresh_in_flight` | Nullable(UInt8) | operator | 1 while a refresh body is currently executing; 0 otherwise. |
| `refresh_started_at` | Nullable(DateTime) | operator | Wall-clock start of the in-flight refresh; NULL when idle. |
| `consecutive_failures` | Nullable(UInt64) | operator | Failed refreshes since the last success; reset to 0 on success. |

## Example

```sql
SELECT name, kind, value, type, current_value_is_valid, consecutive_failures, exception
FROM system.named_scalars;
```

## Operational signals

The following query returns named scalars that need operator attention:

```sql
SELECT
    kind,
    name,
    has_value,
    current_value_is_valid,
    refresh_in_flight,
    refresh_started_at,
    refresh_interval,
    consecutive_failures,
    exception,
    expression
FROM system.named_scalars
WHERE has_value = 0
   OR ifNull(consecutive_failures, 0) > 10
   OR (
        ifNull(refresh_in_flight, 0) = 1
        AND refresh_interval IS NOT NULL
        AND refresh_started_at < now() - toIntervalSecond(refresh_interval)
      )
ORDER BY
    has_value ASC,
    consecutive_failures DESC,
    refresh_started_at ASC,
    kind,
    name;
```

The query reports:

- scalars with no populated value (`has_value = 0`); queries using them will
  throw `NAMED_SCALAR_HAS_NO_VALUE`;
- scalars with more than 10 consecutive refresh failures;
- scalars whose refresh body is still running after its refresh interval.

`refresh_interval` is `NULL` for scalars without a `REFRESH` clause. Operator
columns such as `exception`, `expression`, `refresh_in_flight`, and
`consecutive_failures` are visible only with the `SHOW_NAMED_SCALARS` grant.

:::note Reading scalar values is all-or-nothing
The `getNamedScalar` grant — required for reading scalar values via the
`getNamedScalar` UDF and for the value-tier columns of this table — is
**not per-scalar**. Any role that holds it can read every named scalar's
value. Do not store credentials, secrets, or otherwise narrowly-scoped
sensitive data in named scalars unless every grantee should be able to
read every scalar.
:::

Cumulative refresh counters live in `system.events`:

| Event | Meaning |
|---|---|
| `NamedScalarRefreshAttempts` | Refresh ticks that ran the SELECT. |
| `NamedScalarRefreshSuccesses` | Eval+publish OK. |
| `NamedScalarRefreshFailures` | Eval threw or persist failed. |
| `NamedScalarRefreshSkippedByPeer` | Shared scalar — another replica ran the refresh this tick. |
| `NamedScalarRefreshDurationMicroseconds` | Cumulative wall time of refresh bodies. |

`system.metrics` exposes the dedicated refresh pool's gauges:
`BackgroundNamedScalarRefreshPoolTask` (active refresh tasks) and
`BackgroundNamedScalarRefreshPoolSize` (worker count limit; 0 until
the first `CREATE NAMED SCALAR` triggers lazy pool init).

## Refresh visibility and cancellation

Refresh bodies execute through the standard `executeQuery` path, which means:

- **`system.processes`** lists in-flight refresh SELECTs while they run.
  They can be interrupted with `KILL QUERY` (the scalar then records
  `exception LIKE '%QUERY_WAS_CANCELLED%'` and the previous good value
  remains served).
- **`system.query_log`** contains a `QueryFinish` (or `ExceptionWhileProcessing`)
  row per refresh body, with `is_internal = 1`. Filter on
  `is_internal = 1` if you want to see only refresh queries; exclude it
  if you want to keep refresh queries out of operator dashboards.
- **DROP NAMED SCALAR**, **CREATE OR REPLACE NAMED SCALAR**, and
  server shutdown all interrupt the in-flight refresh body so they don't
  block on a slow SELECT — independent of `KILL QUERY`.
