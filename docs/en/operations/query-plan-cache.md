---
description: 'Guide to using and configuring the experimental query plan cache feature in ClickHouse'
sidebar_label: 'Query plan cache'
sidebar_position: 66
slug: /operations/query-plan-cache
title: 'Query plan cache'
doc_type: 'guide'
---

:::note
The query plan cache is experimental. To use it, both `allow_experimental_query_plan_cache` and `enable_query_plan_cache` must be set to `1`. Only queries that also run with [enable_analyzer](/operations/settings/settings#enable_analyzer) (default) are eligible.
:::

## Background and motivation {#background-and-motivation}

For repeated `SELECT` queries against the same table, ClickHouse spends a non-trivial amount of time on query planning: AST parsing produces an analyzer tree, the analyzer resolves names and types, and the planner produces a `QueryPlan` from which the executable pipeline is built.
On simple OLTP-style workloads (single-table dashboard queries, prepared-statement-style traffic, lightweight point lookups), the planning step itself can dominate end-to-end latency.

The query plan cache stores the serialized `QueryPlan` produced by the analyzer-based planner, keyed by the query AST and the planner-affecting subset of the session settings.
On a subsequent identical query, ClickHouse skips parsing, analysis, and planning, and instead deserializes the cached plan, re-binds it to the current table snapshot, and re-validates access rights before execution.

Unlike the [query cache](query-cache.md), which caches query *results*, the query plan cache caches only the *plan*: every cache hit still executes the query and reads up-to-date data.
This makes the query plan cache transactionally consistent — there is no risk of returning stale rows.

## How it works {#how-it-works}

When a `SELECT` query is admitted, the cached plan is built in two stages so that it can be reused across the table snapshots that exist at insert time vs. lookup time:

- *Universalize*: before serialization, every `ReadFromMergeTree` (and similar storage-bound steps) is replaced with a storage-independent `ReadFromTableStep` that carries only the `StorageID` and the columns to read. Storage-specific state (parts, marks, prewhere actions) is stripped.
- *Materialize*: on a cache hit, the universalized plan is deserialized and `resolveStorages` rebinds each `ReadFromTableStep` to the current `IStorage` snapshot, restoring a directly executable `QueryPlan`.

The cache key includes:

- A 128-bit hash of the normalized query AST.
- A 64-bit hash of the planner-affecting session settings (resource limits, output formatting, logging settings, and a few cache-related settings are excluded — see [`isSettingIgnoredInQueryPlanCache`](https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/QueryPlanCache.cpp)).
- For each table referenced in the query: the table's metadata version (or, for non-replicated tables, a content hash over column names + types + sorting/partition/primary key expressions).
- The hash of the SELECT row policy expression, if any.
- The current user identity and the sorted list of currently active role IDs.

`SYSTEM DROP QUERY PLAN CACHE` invalidates the cache eagerly. Schema changes, role changes, and row policy changes invalidate cache entries lazily — the next lookup will compute a different key and miss.

## Configuration settings and usage {#configuration-settings-and-usage}

Two session settings together gate the feature:

- [`allow_experimental_query_plan_cache`](/operations/settings/settings#allow_experimental_query_plan_cache) — experimental gate (default `0`).
- [`enable_query_plan_cache`](/operations/settings/settings#enable_query_plan_cache) — per-query toggle once the gate is enabled (default `0`).

A third setting controls per-user accounting:

- [`query_plan_cache_size_in_bytes_quota`](/operations/settings/settings#query_plan_cache_size_in_bytes_quota) — admission quota in bytes for the current user. `0` means unlimited.

The maximum total cache size and entry count are controlled by the server-level settings `query_plan_cache.max_size_in_bytes` and `query_plan_cache.max_entries`. Both can be reloaded at runtime via `SYSTEM RELOAD CONFIG`.

Example:

```sql
SELECT a, b FROM hits WHERE EventDate = '2024-01-01'
SETTINGS allow_experimental_query_plan_cache = 1,
         enable_query_plan_cache = 1;
```

The first execution serializes and stores the plan. Subsequent executions of the same query (same AST, same planner-relevant settings, same user/roles, same schema version) skip planning and reuse the cached plan.

## Eligibility {#eligibility}

A query is admitted to the cache only if **all** of the following hold:

- The statement is a single `SELECT` against exactly one table (no `JOIN`, no `UNION` of multiple subqueries, no scalar/`IN`-subquery references to other tables).
- The table is a local, non-system, non-view storage. Distributed tables, table functions, dictionaries, views, and materialized views are excluded.
- [`enable_analyzer`](/operations/settings/settings#enable_analyzer) is `1` (the default).
- The query does not run with parallel replicas (`enable_parallel_replicas = 0`).
- The query does not contain non-deterministic functions (`now`, `rand`, etc.) or subqueries.

Queries that fail any check are still executed normally; they simply do not interact with the cache.

## Limitations and invalidation {#limitations-and-invalidation}

- **Schema changes** invalidate cached plans for the affected table on the next lookup. For replicated tables this happens via the table's `metadata_version`; for non-replicated tables a content hash over the column list and key expressions is used.
- **Row policy changes** invalidate plans on the next lookup because the policy's expression hash is part of the key.
- **User and role changes**: cache entries are scoped to the user identity and the set of currently active roles. A user with different active roles will produce a different key and never share cached plans.
- **Server restart** clears the cache — entries are not persisted to disk.
- **Access rights** are re-checked on every cache hit; revoking `SELECT` after the plan is cached causes the next attempt to fail with `ACCESS_DENIED`.

## Administration {#administration}

To inspect cache state at runtime:

- The number of cache entries and the total bytes used are exposed in [`system.metrics`](/operations/system-tables/metrics) as `QueryPlanCacheEntries` and `QueryPlanCacheBytes`.
- Hit/miss counters since server start are exposed in [`system.events`](/operations/system-tables/events) as `QueryPlanCacheHits` and `QueryPlanCacheMisses`.

To clear the cache:

```sql
SYSTEM DROP QUERY PLAN CACHE;
```

The corresponding privilege is `SYSTEM DROP QUERY PLAN CACHE` (covered by `SYSTEM DROP CACHE`).

## Related content {#related-content}

- [Query cache](query-cache.md) — caches query results, not plans.
- [Query condition cache](query-condition-cache.md) — caches per-granule predicate evaluation outcomes.
