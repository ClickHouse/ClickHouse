---
description: 'Guide to using and configuring the experimental query plan cache in ClickHouse'
sidebar_label: 'Query plan cache'
sidebar_position: 66
slug: /operations/query-plan-cache
title: 'Query plan cache'
doc_type: 'guide'
---

The query plan cache stores the *query plan* of a `SELECT` query so that repeated executions of the same query skip parsing, query
analysis and logical planning. Unlike the [query cache](query-cache.md), which stores query *results*, a query plan cache hit still
executes the query against current data: the cached plan's table reads are storage-agnostic placeholders that are re-bound to fresh data
snapshots on every execution. The plan cache is therefore transactionally consistent and suitable for workloads where the same
analysis-heavy query runs repeatedly over changing data — for example dashboards over deeply nested views.

The feature is experimental:

```sql
SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;
```

## Eligibility {#eligibility}

A `SELECT` query can use the plan cache when all of the following hold:

- The query is a top-level `SELECT` (possibly with `UNION`), run with the analyzer (`enable_analyzer = 1`) and without parallel replicas.
- Queries over **views, joins, subqueries and `IN` subqueries are supported**: views are inlined into the cached plan, and
  subquery/`IN`-set sub-plans re-execute on every run.
- The query contains no non-deterministic functions (e.g. `now`, `rand`), including inside expanded view bodies.
- All referenced storages are local, non-temporary tables or views (no table functions, no `system` tables except `system.one`,
  no `Merge`/`Distributed` storages, no `SQL SECURITY DEFINER` views).
- Scalar subqueries (e.g. `(SELECT max(x) FROM t)`) are evaluated once during analysis and baked into the plan as constants. By default
  such queries are not cached; set `query_plan_cache_allow_scalar_subqueries = 1` to cache them when the tables referenced by scalar
  subqueries are static or stale scalar values are acceptable.

Plans whose steps do not support serialization (e.g. window functions) are executed normally and simply not cached.

## Invalidation {#invalidation}

Every cache hit revalidates the plan's dependencies before execution. The cached entry is discarded and the query re-planned when any
referenced storage (base table or view, including nested views) was dropped, re-created, or altered (schema change, view body change via
`CREATE OR REPLACE VIEW`), or when an applicable row policy changed. The cache key includes the user, the active roles, the current
database, and a hash of all plan-affecting settings, so plans are never shared across users or incompatible settings. `SELECT` access to
all dependencies is re-checked on every hit.

## Configuration {#configuration}

Server configuration:

```xml
<query_plan_cache>
    <max_size_in_bytes>536870912</max_size_in_bytes>
    <max_entries>1024</max_entries>
</query_plan_cache>
```

Per-user quota: `query_plan_cache_size_in_bytes_quota` (0 = no quota). The cache can be cleared with:

```sql
SYSTEM DROP QUERY PLAN CACHE;
```

Monitoring: events `QueryPlanCacheHits`, `QueryPlanCacheMisses`, `QueryPlanCacheValidationMisses`, `QueryPlanCacheStaleMisses` in
[system.events](system-tables/events.md) and per query in `system.query_log.ProfileEvents`; metrics `QueryPlanCacheBytes`,
`QueryPlanCacheEntries` in [system.metrics](system-tables/metrics.md).
