---
description: 'Documentation for CREATE NAMED SCALAR (named cached scalar query result)'
sidebar_label: 'NAMED SCALAR'
sidebar_position: 42
slug: /sql-reference/statements/create/named-scalar
title: 'CREATE NAMED SCALAR'
doc_type: 'reference'
---

Creates a *named cached scalar query result* — a `SELECT` whose single-row,
single-column result is evaluated, cached, and served to subsequent queries as
cheaply as a literal constant. Useful for small, frequently-read values that
are expensive to recompute (aggregates over large tables, values fetched from
remote sources, counters refreshed on a schedule).

Reads go through `getNamedScalar` / `getNamedScalarOrDefault`. The manager
dispatches to the local or shared (Keeper-backed) backend automatically based
on the scalar's recorded cache kind; no separate function is needed for shared
scalars.

## Syntax

```sql
CREATE [OR REPLACE] [LOCAL|SHARED] NAMED SCALAR [IF NOT EXISTS] <name>
    [ON CLUSTER <cluster>]
    [DEFINER = { user | CURRENT_USER }] [SQL SECURITY DEFINER]
    [REFRESH EVERY <N> <unit>]
    AS <SELECT ...>;
```

The `AS` clause must be a `SELECT`. The query must produce exactly one row,
one column at evaluation time — empty or multi-row results are errors.

`<unit>` is one of `SECOND`, `MINUTE`, `HOUR`, `DAY` (with optional plural).
A failed refresh retries on the next scheduled tick — there is no separate
backoff. The previously-good value continues to be served while the refresh
is broken (`current_value_is_valid` flips to 0; `consecutive_failures` bumps).

Named scalars always use `SQL SECURITY DEFINER`: the source query is evaluated
with the privileges of the definer user, both during creation and during
refresh. If `DEFINER` is omitted, `CURRENT_USER` is used. `SQL SECURITY INVOKER`
and `SQL SECURITY NONE` are not supported for named scalars.

## Cache Kinds

| Modifier | Value cache | Definition store |
|---|---|---|
| (none) | Uses `<default_named_scalar_cache>` (`local` by default) | selected at server start |
| `LOCAL` | per-server disk cache under `<named_scalar_local_cache_path>` (default `<data>/named_scalars_cache/`) | selected at server start |
| `SHARED` | cluster-wide Keeper cache, leader-elected refresh | Keeper-backed definitions only |

`LOCAL` / `SHARED` is a value-cache property, not a separate namespace. The
server has one active definition store: disk via `<named_scalar_definitions_path>`
or Keeper via `<named_scalar_definitions_zookeeper_path>`. `SHARED` cache
requires the Keeper definition store.
`SHARED` cannot be combined with `ON CLUSTER` — Keeper already distributes the
scalar across the cluster.

## OR REPLACE

`CREATE OR REPLACE` atomically swaps the definition and value: in-flight
queries see either the previous or the new scalar, never an intermediate
state. The new SELECT result is what gets published; nothing carries over
from the previous definition (including its declared type).

## Examples

A per-node computed value, refreshed every hour:

```sql
CREATE NAMED SCALAR fx_rate
    REFRESH EVERY 1 HOUR
    AS SELECT rate FROM rates WHERE pair = 'EUR/USD';

SELECT amount * getNamedScalar('fx_rate') FROM orders;
```

A cluster-wide watermark shared via Keeper:

```sql
CREATE SHARED NAMED SCALAR max_event_time
    REFRESH EVERY 1 MINUTE
    AS SELECT max(event_time) FROM events;

SELECT count() FROM events WHERE event_time > getNamedScalar('max_event_time');
```

Fan-out to every node in a cluster (local scope):

```sql
CREATE NAMED SCALAR node_name ON CLUSTER my_cluster AS SELECT hostName();
-- each node evaluates hostName() on itself; values differ across nodes.
```

Default fallback when the scalar isn't defined:

```sql
SELECT getNamedScalarOrDefault('flap', 0);
```

## When to use

A named scalar is the right tool when the value (a) needs background
evaluation with no query to attach to, or (b) needs to swap atomically
across a cluster, or (c) is computed under privileges the reader
doesn't have. The patterns below all lean on at least one of those.

**ETL high-water mark across replicas (Shared).** A SHARED scalar
holds the last successfully-loaded id; every replica reads it,
exactly one replica refreshes it after a batch lands. No app-side
cursor that drifts between nodes.

```sql
CREATE SHARED NAMED SCALAR last_loaded_id
    REFRESH EVERY 30 SECOND
    AS SELECT max(event_id) FROM staging.events_loaded;

INSERT INTO main.events
    SELECT * FROM staging.events
    WHERE event_id > getNamedScalar('last_loaded_id');
```

**Cached top-N (or any small set) as an inline filter.** Refresh
hourly; reads are constant-time. Without scalars this is either a
correlated subquery scanning the fact table on every dashboard hit
or a one-row table with `ARRAY JOIN`.

```sql
CREATE NAMED SCALAR top_1k_users
    REFRESH EVERY 1 HOUR
    AS SELECT groupArray(user_id) FROM (
        SELECT user_id FROM events
        GROUP BY user_id ORDER BY count() DESC LIMIT 1000);

SELECT * FROM events WHERE user_id IN getNamedScalar('top_1k_users');
```

**Slow-changing reference values.** FX rates, holiday calendars, the
current fiscal period, p99 baselines. The atomic single-value swap
sidesteps the "what does an in-flight query see while the table is
being rewritten" question that comes with a tiny reference table +
`OPTIMIZE FINAL`.

**Anomaly / alert thresholds derived from rolling history.** The
threshold is a query over `system.query_log` (or wherever); the alert
predicate uses it as a literal. The whole feedback loop lives inside
the database.

```sql
CREATE NAMED SCALAR p99_24h
    REFRESH EVERY 1 HOUR
    AS SELECT quantile(0.99)(query_duration_ms)
       FROM system.query_log WHERE event_time > now() - INTERVAL 1 DAY;

SELECT * FROM live_queries WHERE query_duration_ms > getNamedScalar('p99_24h');
```

**Salt / key rotation for PII hashing.** Materialized views and
visible columns reference `getNamedScalar('salt_v')`; rotating the
salt is one `CREATE OR REPLACE`. No DDL churn across dependent
objects, no coordinated cutover.

**Cheap broadcast of expensive estimates.** A `uniqHLL12` /
approximate-quantile / Bloom summary computed minutes-long and read
microseconds. Stored once as a scalar, used to short-circuit
planning logic in views. Without it: re-computed per query, or
maintained as a one-row `ReplacingMergeTree` with all the staleness
and atomicity caveats that brings.

**Last-known-good model coefficients for inline scoring.** A nightly
training job writes the coefficients via `CREATE OR REPLACE`. The
atomic swap means no half-updated coefficient set is ever readable;
queries see either the old set or the new set.

```sql
CREATE OR REPLACE NAMED SCALAR scoring_intercept AS SELECT 0.42;
CREATE OR REPLACE NAMED SCALAR scoring_alpha     AS SELECT 1.7;

SELECT
    getNamedScalar('scoring_alpha') * x + getNamedScalar('scoring_intercept')
        AS score
FROM features;
```

**Feature flags / experiment cohorts.** `cohort_pct` as a SHARED
scalar; `multiIf(user_id % 100 < getNamedScalar('cohort_pct'),
'A', 'B')`. Toggle without redeploying DDL; atomic across the
cluster.

**Privilege-bridge for sensitive aggregates.** Admin defines
`total_revenue AS SELECT sum(amount) FROM orders`; analysts have
`getNamedScalar` but not `SELECT` on `orders`. The DEFINER-style
refresh removes the read-only summary table + grant ladder
bookkeeping that would otherwise be needed.

```sql
CREATE NAMED SCALAR total_revenue
    DEFINER = analytics_admin SQL SECURITY DEFINER
    REFRESH EVERY 5 MINUTE
    AS SELECT sum(amount) FROM orders;
```

A small reference table is usually a better fit for *many-row*
data. A named scalar is for the single-value case where (a)/(b)/(c)
above outweigh the natural relational shape.

## Persistence

`CREATE` validates the source query by running it once, then persists the
definition. The first cached value is populated asynchronously by the
background refresh machinery; reads can throw `NAMED_SCALAR_HAS_NO_VALUE`
until that first populate succeeds.

Definitions are stored in exactly one active store per server. If
`<named_scalar_definitions_zookeeper_path>` is configured, definitions are
cluster-wide in Keeper. Otherwise definitions are local files under
`<named_scalar_definitions_path>` (default `<data>/named_scalars/`).

Local cached values are stored on this server under
`<named_scalar_local_cache_path>` (default `<data>/named_scalars_cache/`).
Shared cached values are stored cluster-wide under the same Keeper root as
the definitions. Refresh is leader-elected:
exactly one replica per initial populate or scheduled tick runs the
SELECT and publishes the result, and every other replica picks up the
published value without re-evaluating. `CREATE`, `DROP`, and `OR REPLACE` of a
Keeper-backed scalar are atomic from every replica's perspective.

## Server configuration

```xml
<clickhouse>
    <!-- Pick one definition store. If neither is set, disk definitions
         default to <data>/named_scalars/. -->
    <named_scalar_definitions_path>/var/lib/clickhouse/named_scalars/</named_scalar_definitions_path>
    <!-- OR -->
    <named_scalar_definitions_zookeeper_path>/clickhouse/named_scalars</named_scalar_definitions_zookeeper_path>

    <!-- Local value cache. Default: <data>/named_scalars_cache/. -->
    <named_scalar_local_cache_path>/var/lib/clickhouse/named_scalars_cache/</named_scalar_local_cache_path>

    <!-- Cache kind used when CREATE omits LOCAL/SHARED. Default: local.
         'shared' requires <named_scalar_definitions_zookeeper_path>. -->
    <default_named_scalar_cache>local</default_named_scalar_cache>

    <!-- Maximum encoded value size (bytes). Default 1 MiB. -->
    <named_scalar_max_value_size>1048576</named_scalar_max_value_size>

    <!-- Dedicated thread pool for refresh tasks. Lazily instantiated on
         the first CREATE NAMED SCALAR; servers without any named scalars
         pay no thread overhead. Isolated from the general
         background_schedule_pool so a slow/hung refresh body cannot
         starve replication, Kafka streaming, or DNS-cache tasks. -->
    <background_named_scalar_refresh_pool_size>16</background_named_scalar_refresh_pool_size>
</clickhouse>
```

## Access control

- `CREATE_NAMED_SCALAR` — required for `CREATE [OR REPLACE] NAMED SCALAR`.
- `DROP_NAMED_SCALAR` — required for `DROP NAMED SCALAR`.
- `SET DEFINER` on the target user — required to create a named scalar with
  `DEFINER = <user>` when `<user>` is not the current user.
- `getNamedScalar` (function-execute grant) — required for `getNamedScalar` and
  `getNamedScalarOrDefault`.
- `SHOW_NAMED_SCALARS` — required for unrestricted `system.named_scalars`
  reads.
- `SYSTEM REFRESH NAMED SCALAR` — required for `SYSTEM REFRESH NAMED SCALAR`.
- `SYSTEM NAMED SCALAR REFRESHES` — required for
  `SYSTEM START NAMED SCALAR REFRESHES` and `SYSTEM STOP NAMED SCALAR REFRESHES`.

:::note
The `getNamedScalar` grant is **all-or-nothing**: any role that holds it
can read the value of every named scalar on the server. There is no
per-scalar ACL. Do not store secrets, credentials, or other
narrowly-scoped sensitive data in named scalars unless every grantee
should be able to read every scalar.
:::

## Operating shared (Keeper-backed) scalars

`SHARED` scalars store both their definitions and their cached values
in Keeper, under `<named_scalar_definitions_zookeeper_path>`. The named
scalar manager trusts this Keeper subtree the same way ClickHouse
trusts the Keeper paths of `Database engine = Replicated` and
`ReplicatedMergeTree` — anyone with write access to the path can mutate
metadata, including the `DEFINER` clause that determines under which
identity the refresh `SELECT` runs.

**Operationally that means: the named-scalar Keeper subtree must be
ACL-restricted to the ClickHouse server identity.** Use Keeper ACLs to
deny write access to anyone other than the server's own credentials.
This is the same requirement that already applies to replicated table
metadata; it is not a new responsibility introduced by named scalars.

## See also

- [`DROP NAMED SCALAR`](/sql-reference/statements/drop#drop-named-scalar) — removes a scalar.
- [`SYSTEM REFRESH NAMED SCALAR`](/sql-reference/statements/system#refresh-named-scalar) — re-runs the
  source query on demand.
- [`SYSTEM STOP / START NAMED SCALAR REFRESHES`](/sql-reference/statements/system#stop-named-scalar-refreshes) — pause/resume periodic refresh on the current server.
- [`getNamedScalar` / `getNamedScalarOrDefault`](/sql-reference/functions/named-scalar-functions) — functions to read scalar values.
- [`system.named_scalars`](/operations/system-tables/named_scalars) — runtime
  introspection.
- [`CREATE MATERIALIZED VIEW`](/sql-reference/statements/create/view) — for
  cached *relational* (many-row) results.
