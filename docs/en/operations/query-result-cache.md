---
slug: /en/operations/query-result-cache
sidebar_position: 65
sidebar_label: Query Result Cache [experimental]
---

# Query Result Cache [experimental]

The query result cache allows to compute SELECT queries just once and to serve further executions of the same query directly from the cache.
Depending on the type of the queries, this can dramatically reduce latency and resource consumption of the ClickHouse server.

## Background, Design and Limitations

Query result caches can generally be viewed as transactionally consistent or inconsistent.

- In transactionally consistent caches, the database invalidates (discards) cached query results if the result of the SELECT query changes
  or potentially changes. In ClickHouse, operations which change the data include inserts/updates/deletes in/of/from tables or collapsing
  merges. Transactionally consistent caching is especially suitable for OLTP databases, for example
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (which removed query result cache after v8.0) and
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
- In transactionally inconsistent caches, slight inaccuracies in query results are accepted under the assumption that all cache entries are
  assigned a validity period after which they expire (e.g. 1 minute) and that the underlying data changes only little during this period.
  This approach is overall more suitable for OLAP databases. As an example where transactionally inconsistent caching is sufficient,
  consider an hourly sales report in a reporting tool which is simultaneously accessed by multiple users. Sales data changes typically
  slowly enough that the database only needs to compute the report once (represented by the first SELECT query). Further queries can be
  served directly from the query result cache. In this example, a reasonable validity period could be 30 min.

Transactionally inconsistent caching is traditionally provided by client tools or proxy packages interacting with the database. As a result,
the same caching logic and configuration is often duplicated. With ClickHouse's query result cache, the caching logic moves to the server
side. This reduces maintenance effort and avoids redundancy.

:::warning
The query result cache is an experimental feature that should not be used in production. There are known cases (e.g. in distributed query
processing) where wrong results are returned.
:::

## Configuration Settings and Usage

Parameter [enable_experimental_query_result_cache](settings/settings.md#enable-experimental-query-result-cache) controls whether query
results are inserted into / retrieved from the cache for the current query or session. For example, the first execution of query

``` sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS enable_experimental_query_result_cache = true;
```

stores the query result into the query result cache. Subsequent executions of the same query (also with parameter
`enable_experimental_query_result_cache = true`) will read the computed result directly from the cache.

Sometimes, it is desirable to use the query result cache only passively, i.e. to allow reading from it but not writing into it (if the cache
result is not stored yet). Parameter [enable_experimental_query_result_cache_passive_usage](settings/settings.md#enable-experimental-query-result-cache-passive-usage)
instead of 'enable_experimental_query_result_cache' can be used for that.

For maximum control, it is generally recommended to provide settings "enable_experimental_query_result_cache" or
"enable_experimental_query_result_cache_passive_usage" only with specific queries. It is also possible to enable caching at user or profile
level but one should keep in mind that all SELECT queries may return a cached results, including monitoring or debugging queries to system
tables.

The query result cache can be cleared using statement `SYSTEM DROP QUERY RESULT CACHE`. The content of the query result cache is displayed
in system table `SYSTEM.QUERY_RESULT_CACHE`. The number of query result cache hits and misses are shown as events "QueryResultCacheHits" and
"QueryResultCacheMisses" in system table `SYSTEM.EVENTS`. Both counters are only updated for SELECT queries which run with settings
"enable_experimental_query_result_cache = true" or "enable_experimental_query_result_cache_passive_usage = true". Other queries do not
affect the cache miss counter.

The query result cache exists once per ClickHouse server process. However, cache results are by default not shared between users. This can
be changed (see below) but doing so is not recommended for security reasons.

Query results are referenced in the query result cache by the [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
of their query. This means that caching is agnostic to upper/lowercase, for example `SELECT 1` and `select 1` are treated as the same query.
To make the matching more natural, all query-level settings related to the query result cache are removed from the AST.

If the query was aborted due to an exception or user cancellation, no entry is written into the query result cache.

The size of the query result cache, the maximum number of cache entries and the maximum size of cache entries (in bytes and in records) can
be configured using different [server configuration options](server-configuration-parameters/settings.md#server_configuration_parameters_query-result-cache).

To define how long a query must run at least such that its result can be cached, you can use setting
[query_result_cache_min_query_duration](settings/settings.md#query-result-cache-min-query-duration). For example, the result of query

``` sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS enable_experimental_query_result_cache = true, query_result_cache_min_query_duration = 5000;
```

is only cached if the query runs longer than 5 seconds. It is also possible to specify how often a query needs to run until its result is
cached - for that use setting [query_result_cache_min_query_runs](settings/settings.md#query-result-cache-min-query-runs).

Entries in the query result cache become stale after a certain time period (time-to-live). By default, this period is 60 seconds but a
different value can be specified at session, profile or query level using setting [query_result_cache_ttl](settings/settings.md#query-result-cache-ttl).

Also, results of queries with non-deterministic functions such as `rand()` and `now()` are not cached. This can be overruled using
setting [query_result_cache_store_results_of_queries_with_nondeterministic_functions](settings/settings.md#query-result-cache-store-results-of-queries-with-nondeterministic-functions).

Finally, entries in the query cache are not shared between users due to security reasons. For example, user A must not be able to bypass a
row policy on a table by running the same query as another user B for whom no such policy exists. However, if necessary, cache entries can
be marked accessible by other users (i.e. shared) by supplying setting
[query_result_cache_share_between_users]{settings/settings.md#query-result-cache-share-between-users}.
