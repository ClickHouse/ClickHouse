---
description: 'Guide to using and configuring the query plan cache feature in ClickHouse'
sidebar_label: 'Query plan cache'
sidebar_position: 66
slug: /operations/query-plan-cache
title: 'Query plan cache'
doc_type: 'guide'
---

:::note
The query plan cache is experimental. To use it, set `enable_query_plan_cache` to `1`. Only queries that also run with [enable_analyzer](/operations/settings/settings#enable_analyzer) (default) are eligible.
:::

## Background and motivation {#background-and-motivation}

For repeated `SELECT` queries against the same table, ClickHouse may spend a non-trivial amount of time on query planning: AST parsing produces an analyzer tree, the analyzer resolves names and types, and the planner produces a query plan from which the executable pipeline is built.
On simple OLTP-style workloads (single-table dashboard queries, prepared-statement-style traffic, lightweight point lookups), the planning step itself can dominate end-to-end latency.

The query plan cache stores the serialized query plan produced by the analyzer-based planner, keyed by the query AST and the planner-affecting subset of the session settings.
On a subsequent identical query, ClickHouse still parses the SQL text to build the cache lookup key. If a matching entry is found and its dependencies are still valid, ClickHouse skips analysis and planning, deserializes the cached plan, re-binds it to the current table snapshot, and re-validates access rights before execution.

Unlike the [query cache](query-cache.md), which caches query *results*, the query plan cache caches only the *plan*: every cache hit still executes the query and reads up-to-date data.
This makes the query plan cache transactionally consistent — there is no risk of returning stale rows.

## How it works {#how-it-works}

When a `SELECT` query is admitted, the cached plan is built in two stages so that it can be reused with newer compatible table snapshots:

- *Universalize*: before serialization, storage-specific read state such as parts and marks is replaced with a logical table read that retains the required columns and query modifiers.
- *Materialize*: on a cache hit, ClickHouse validates the current table definition, acquires a current snapshot, and rebuilds the storage reads to produce an executable query plan.

The lookup key is built before query analysis. A found entry is treated as a candidate and is executed only after dependency validation succeeds. Validation compares the current storage engine, the exact descriptions of columns used by the plan, and the sorting, partitioning, primary-key, and sampling properties against the cached read contract. If that contract is incompatible, ClickHouse performs normal analysis and planning and replaces the entry.

The cache key includes:

- The exact canonical representation of the normalized query AST. A 128-bit AST hash is used only to accelerate lookup; equality never relies on the hash alone.
- A 64-bit hash of the planner-affecting session settings (resource limits, output formatting, logging settings, and a few cache-related settings are excluded — see [`isSettingIgnoredInQueryPlanCache`](https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/QueryPlanCache.cpp)).

The key does not contain a user identity or table UUID, so compatible plans can be shared across users and across a compatible `DROP TABLE` followed by `CREATE TABLE` of the same logical table. Every hit revalidates `SELECT` privileges for the current user and binds the plan to the current storage snapshot. `SYSTEM DROP QUERY PLAN CACHE` invalidates the cache eagerly.

## Configuration settings and usage {#configuration-settings-and-usage}

The experimental session setting [`enable_query_plan_cache`](/operations/settings/settings#enable_query_plan_cache) controls the feature and defaults to `0`.

The maximum total cache size and entry count are controlled by the server-level settings `query_plan_cache.max_size_in_bytes` and `query_plan_cache.max_entries`. The default maximum size is `100 MiB`. Both settings can be reloaded at runtime via `SYSTEM RELOAD CONFIG`.

Example:

```sql
SELECT a, b FROM hits WHERE EventDate = '2024-01-01'
SETTINGS enable_query_plan_cache = 1;
```

The first execution serializes and stores the plan. Subsequent executions of the same query with the same canonical AST and planner-relevant settings can skip planning when the current storage dependency contract is compatible.

## Eligibility {#eligibility}

A query is admitted to the cache only if **all** of the following hold:

- The statement is a single `SELECT` against exactly one table (no `JOIN`, no `UNION` of multiple subqueries, no scalar/`IN`-subquery references to other tables).
- The table is a direct local, non-system, non-view `MergeTree`-family storage. Distributed tables, table functions, dictionaries, views, materialized views, and wrapper engines such as `Buffer` or `Merge` are excluded.
- [`enable_analyzer`](/operations/settings/settings#enable_analyzer) is `1` (the default).
- The query does not run with parallel replicas (`enable_parallel_replicas = 0`).
- The query does not contain non-deterministic functions (`now`, `rand`, etc.) or subqueries.
- No row policy applies to the current user for the table. Queries with any applicable row policy, including an always-true policy, bypass the plan cache.

Queries that fail any check are still executed normally; they simply do not interact with the cache.

## Limitations and invalidation {#limitations-and-invalidation}

- **Schema changes** are checked against the columns and key properties used by the cached read. Changes to an unrelated column can remain compatible, while changes to a required column type, default expression, storage engine, sorting key, partition key, primary key, or sampling key reject the candidate.
- **Row policies** are not supported by the experimental cache. A query with an applicable policy does not look up or populate the cache.
- **Users and roles** are not part of the key. Plans can be shared across users, but table- and column-level `SELECT` privileges are checked for the current user on every hit.
- **Server restart** clears the cache — entries are not persisted to disk.
- **Access rights** are re-checked on every cache hit. The current user must retain `SELECT` on all columns selected by query semantics, or on at least one column for queries such as `SELECT count()` that do not select a storage column.

## Administration {#administration}

To inspect cache state at runtime:

- The number of cache entries and the total bytes used are exposed in [`system.metrics`](/operations/system-tables/metrics) as `QueryPlanCacheEntries` and `QueryPlanCacheBytes`.
- Hit/miss counters since server start are exposed in [`system.events`](/operations/system-tables/events) as `QueryPlanCacheHits` and `QueryPlanCacheMisses`. `QueryPlanCacheHits` means a candidate entry was found, while `QueryPlanCacheValidationMisses` counts candidates rejected by dependency validation.

To clear the cache:

```sql
SYSTEM DROP QUERY PLAN CACHE;
```

The corresponding privilege is `SYSTEM DROP QUERY PLAN CACHE` (covered by `SYSTEM DROP CACHE`).

## Related content {#related-content}

- [Query cache](query-cache.md) — caches query results, not plans.
- [Query condition cache](query-condition-cache.md) — caches per-granule predicate evaluation outcomes.
