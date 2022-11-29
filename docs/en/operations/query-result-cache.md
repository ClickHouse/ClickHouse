---
slug: /en/operations/caches
sidebar_position: 65
sidebar_label: Query Result Cache [experimental]
title: "Query Result Cache [experimental]"
---

# Query Result Cache [experimental]

The query result cache is an experimental feature which can speed up repeated executions of the same SELECT query.

## Background, Design and Limitations

Query caches are generally either transactionally consistent or inconsistent.

- In transactionally consistent caches, the database invalidates/discards cached query results if the result of the SELECT query changes or
  even potentially changes. In ClickHouse, operations which change the data include for example inserts/updates/deletes in/of/from tables or
  collapsing merges. Transactionally consistent caching is especially suitable for OLTP databases, for example
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (which removed query result cache after v8.0) and
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
- In transactionally inconsistent caches, invalid query results are generally accepted under the assumption that all cache entries are
  assigned a validity period after which they expire (e.g. 1 minute) and that the underlying data changes only little during this period.
  This approach is overall more suitable for OLAP databases. As an example where transactionally inconsistent caching is sufficient,
  consider a scenario in which multiple users simultaneously access the same hourly sales report (represented by the same SELECT query) in a
  reporting tool. Sales data typically changes slowly enough that the database only needs to actually compute the first report, further
  queries can be answered from the query result cache. As validity period, for example, 30 min can be used.

Transactionally inconsistent caching is traditionally provided by the clients interacting with the database, meaning that different tools
need to re-implement similar caching logic and configuration. With the (experimental) query result cache in ClickHouse, the caching logic is
moved to the server side. This reduces maintenance effort and avoids duplication.

## Usage Examples and Configuration Settings

The two query/user/profile-level parameters [experimental_query_result_cache_active_usage](../../operations/settings/settings.md#setting-experimental_query_result_cache_active_usage)
and [experimental_query_result_cache_passive_usage](../../operations/settings/settings.md#setting-experimental_query_result_cache_passive_usage) control whether query results are
inserted or retrieved from the cache. For example, the first execution of query

``` sql
SELECT expensive_calculation(A, B, C)
FROM T
SETTINGS experimental_query_result_cache_active_usage = true, experimental_query_result_cache_passive_usage = false;
```

will store the query result into the query result cache and subsequent executions will retrieve the result directly from the cache.

For maximum control, it is generally recommended to enable caching on a per-query basis. It is also possible to activate caching at
user/profile level if the user keeps in mind that all SELECT queries may return outdated results then.

To clear the query result cache, use statement `SYSTEM DROP QUERY RESULT CACHE`. The content of the query result cache is displayed in
system table `SYSTEM.QUERYRESULT_CACHE`.

The cache exists per-user and cache results are not shared between users.

SELECT queries which contain non-deterministic functions and predicates (e.g. `now()`, `rand()`, etc.) are not cached.

Queries are indexed in the cache by their AST, meaning that caching is agnostic to upper/lowercase (`SELECT` vs. `select`).

### Further configuration options:

To configure the size of the query result cache, use setting [query_result_cache_size](settings/settings.md#query-result-cache-size).

To control how often a query needs to run until its result is cached, use setting [query_result_cache_min_query_runs](settings/settings.md#query-result-cache-min-query-runs).

To set the maximum size of cache entries, use settings [query_result_cache_max_entry_size](settings/settings.md#query-result-cache-max-entry-size).

To specify the validity period after which cache entries become stale, use setting [query_result_cache_keep_seconds_alive](settings/settings.md#query-result-cache-keep-seconds-alive).

Finally, it is sometimes useful to cache query results of the same query multiple times with different validity periods. To identify
different entries for the same query, users may pass configuration [query_result_cache_partition_key](settings/settings.md#query-result-cache-partition-key).



