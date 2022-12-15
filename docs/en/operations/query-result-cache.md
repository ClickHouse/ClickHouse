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
  potentially changes. In ClickHouse, operations which change the data include inserts/updates/deletes in/of/from tables or collapsing
  merges. Transactionally consistent caching is especially suitable for OLTP databases, for example
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (which removed query result cache after v8.0) and
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
- In transactionally inconsistent caches, slight inaccuracies in query results are accepted under the assumption that all cache entries are
  assigned a validity period after which they expire (e.g. 1 minute) and that the underlying data changes only little during this period.
  This approach is overall more suitable for OLAP databases. As an example where transactionally inconsistent caching is sufficient,
  consider an hourly sales report in a reporting tool (represented by a SELECT query) which is simultaneously accessed by multiple users.
  Sales data changes typically slowly enough that the database only needs to compute . Further queries can be served
  directly from the query result cache. In this example, a reasonable validity period could be 30 min.

Transactionally inconsistent caching is traditionally provided by client tools or proxy packages interacting with the database. As a result,
the same caching logic and configuration is often duplicated. With ClickHouse's query result cache, the caching logic moves to the server
side. This reduces maintenance effort and avoids redundance.

## Usage Examples and Configuration Settings

The query/user/profile-level parameter [enable_experimental_query_result_cache](../../operations/settings/settings.md#enable-experimental-enable-query-result-cache)
controls whether query results are inserted or retrieved from the cache. For example, the first execution of query

``` sql
SELECT expensive_calculation(A, B, C)
FROM T
SETTINGS enable_experimental_query_result_cache = true;
```

will store the query result into the query result cache and subsequent executions will retrieve the result directly from the cache.

It is sometimes desirable to use the query result cache only passively, i.e. to read from it but not write in it. Parameter
[enable_experimental_query_result_cache_passive_usage](../../operations/settings/settings.md#enable-experimental-enable-query-result-cache-passive-usage)
instead of 'enable_experimental_query_result_cache' can be used for that.

For maximum control, it is generally recommended to enable caching on a per-query basis. It is also possible to activate caching at
user/profile level but users should keep in mind that all SELECT queries may then return outdated results.

To clear the query result cache, use statement `SYSTEM DROP QUERY RESULT CACHE`. The content of the query result cache is displayed in
system table `SYSTEM.QUERYRESULT_CACHE`.

The cache exists once per ClickHouse server process but cache results are by default not shared between users (see below).

Query results are referenced in the cache by the AST of their query. This means that caching is agnostic to upper/lowercase, for example `SELECT 1` and `select 1` are treated as the same query.

### Further Configuration Options:

To configure the size of the query result cache, use setting [query_result_cache_size](settings/settings.md#query-result-cache-size).

To set the maximum number of cache entries and the maximum size of a cache entry in bytes and in records, use settings [query_result_cache_max_entries](settings/settings.md#query-result-cache-max-entries), [query_result_cache_max_entry_size](settings/settings.md#query-result-cache-max-entry-size) and [query_result_cache_max_entry_records](settings/settings.md#query-result-cache-max-entry-records).

To control how often a query needs to run until its result is cached, use setting [query_result_cache_min_query_runs](settings/settings.md#query-result-cache-min-query-runs).

To specify the validity period after which cache entries become stale, use setting [query_result_cache_keep_seconds_alive](settings/settings.md#query-result-cache-keep-seconds-alive).

Results of queries with non-deterministic functions such as `rand()` and `now()` are not cached by default. This behavior can be overruled using setting [query_result_cache_ignore_nondeterministic_functions](settings/settings.md#query-result-cache-ignore-nondeterministic-functions).

Finally, it is sometimes useful to cache query results of the same query multiple times with different validity periods. To identify
different entries for the same query, users may pass configuration [query_result_cache_partition_key](settings/settings.md#query-result-cache-partition-key).
