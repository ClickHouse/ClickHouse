---
slug: /en/operations/query-cache
sidebar_position: 65
sidebar_label: Query Cache
---

# Query Cache

The query cache allows to compute `SELECT` queries just once and to serve further executions of the same query directly from the cache.
Depending on the type of the queries, this can dramatically reduce latency and resource consumption of the ClickHouse server.

## Background, Design and Limitations

Query caches can generally be viewed as transactionally consistent or inconsistent.

- In transactionally consistent caches, the database invalidates (discards) cached query results if the result of the `SELECT` query changes
  or potentially changes. In ClickHouse, operations which change the data include inserts/updates/deletes in/of/from tables or collapsing
  merges. Transactionally consistent caching is especially suitable for OLTP databases, for example
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (which removed query cache after v8.0) and
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
- In transactionally inconsistent caches, slight inaccuracies in query results are accepted under the assumption that all cache entries are
  assigned a validity period after which they expire (e.g. 1 minute) and that the underlying data changes only little during this period.
  This approach is overall more suitable for OLAP databases. As an example where transactionally inconsistent caching is sufficient,
  consider an hourly sales report in a reporting tool which is simultaneously accessed by multiple users. Sales data changes typically
  slowly enough that the database only needs to compute the report once (represented by the first `SELECT` query). Further queries can be
  served directly from the query cache. In this example, a reasonable validity period could be 30 min.

Transactionally inconsistent caching is traditionally provided by client tools or proxy packages interacting with the database. As a result,
the same caching logic and configuration is often duplicated. With ClickHouse's query cache, the caching logic moves to the server side.
This reduces maintenance effort and avoids redundancy.

## Configuration Settings and Usage

:::note
In ClickHouse Cloud, you must use [query level settings](/en/operations/settings/query-level) to edit query cache settings. Editing [config level settings](/en/operations/configuration-files) is currently not supported.
:::

Setting [use_query_cache](settings/settings.md#use-query-cache) can be used to control whether a specific query or all queries of the
current session should utilize the query cache. For example, the first execution of query

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

will store the query result in the query cache. Subsequent executions of the same query (also with parameter `use_query_cache = true`) will
read the computed result from the cache and return it immediately.

:::note
Setting `use_query_cache` and all other query-cache-related settings only take an effect on stand-alone `SELECT` statements. In particular,
the results of `SELECT`s to views created by `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` are not cached unless the `SELECT`
statement runs with `SETTINGS use_query_cache = true`.
:::

The way the cache is utilized can be configured in more detail using settings [enable_writes_to_query_cache](settings/settings.md#enable-writes-to-query-cache)
and [enable_reads_from_query_cache](settings/settings.md#enable-reads-from-query-cache) (both `true` by default). The former setting
controls whether query results are stored in the cache, whereas the latter setting determines if the database should try to retrieve query
results from the cache. For example, the following query will use the cache only passively, i.e. attempt to read from it but not store its
result in it:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

For maximum control, it is generally recommended to provide settings `use_query_cache`, `enable_writes_to_query_cache` and
`enable_reads_from_query_cache` only with specific queries. It is also possible to enable caching at user or profile level (e.g. via `SET
use_query_cache = true`) but one should keep in mind that all `SELECT` queries may return cached results then.

The query cache can be cleared using statement `SYSTEM DROP QUERY CACHE`. The content of the query cache is displayed in system table
[system.query_cache](system-tables/query_cache.md). The number of query cache hits and misses since database start are shown as events
"QueryCacheHits" and "QueryCacheMisses" in system table [system.events](system-tables/events.md). Both counters are only updated for
`SELECT` queries which run with setting `use_query_cache = true`, other queries do not affect "QueryCacheMisses". Field `query_cache_usage`
in system table [system.query_log](system-tables/query_log.md) shows for each executed query whether the query result was written into or
read from the query cache. Asynchronous metrics "QueryCacheEntries" and "QueryCacheBytes" in system table
[system.asynchronous_metrics](system-tables/asynchronous_metrics.md) show how many entries / bytes the query cache currently contains.

The query cache exists once per ClickHouse server process. However, cache results are by default not shared between users. This can be
changed (see below) but doing so is not recommended for security reasons.

Query results are referenced in the query cache by the [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of
their query. This means that caching is agnostic to upper/lowercase, for example `SELECT 1` and `select 1` are treated as the same query. To
make the matching more natural, all query-level settings related to the query cache are removed from the AST.

If the query was aborted due to an exception or user cancellation, no entry is written into the query cache.

The size of the query cache in bytes, the maximum number of cache entries and the maximum size of individual cache entries (in bytes and in
records) can be configured using different [server configuration options](server-configuration-parameters/settings.md#server_configuration_parameters_query-cache).

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

It is also possible to limit the cache usage of individual users using [settings profiles](settings/settings-profiles.md) and [settings
constraints](settings/constraints-on-settings.md). More specifically, you can restrict the maximum amount of memory (in bytes) a user may
allocate in the query cache and the maximum number of stored query results. For that, first provide configurations
[query_cache_max_size_in_bytes](settings/settings.md#query-cache-max-size-in-bytes) and
[query_cache_max_entries](settings/settings.md#query-cache-max-entries) in a user profile in `users.xml`, then make both settings
readonly:

``` xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

To define how long a query must run at least such that its result can be cached, you can use setting
[query_cache_min_query_duration](settings/settings.md#query-cache-min-query-duration). For example, the result of query

``` sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

is only cached if the query runs longer than 5 seconds. It is also possible to specify how often a query needs to run until its result is
cached - for that use setting [query_cache_min_query_runs](settings/settings.md#query-cache-min-query-runs).

Entries in the query cache become stale after a certain time period (time-to-live). By default, this period is 60 seconds but a different
value can be specified at session, profile or query level using setting [query_cache_ttl](settings/settings.md#query-cache-ttl).

Entries in the query cache are compressed by default. This reduces the overall memory consumption at the cost of slower writes into / reads
from the query cache. To disable compression, use setting [query_cache_compress_entries](settings/settings.md#query-cache-compress-entries).

Sometimes it is useful to keep multiple results for the same query cached. This can be achieved using setting
[query_cache_tag](settings/settings.md#query-cache-tag) that acts as as a label (or namespace) for a query cache entries. The query cache
considers results of the same query with different tags different.

Example for creating three different query cache entries for the same query:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

To remove only entries with tag `tag` from the query cache, you can use statement `SYSTEM DROP QUERY CACHE TAG 'tag'`.

ClickHouse reads table data in blocks of [max_block_size](settings/settings.md#setting-max_block_size) rows. Due to filtering, aggregation,
etc., result blocks are typically much smaller than 'max_block_size' but there are also cases where they are much bigger. Setting
[query_cache_squash_partial_results](settings/settings.md#query-cache-squash-partial-results) (enabled by default) controls if result blocks
are squashed (if they are tiny) or split (if they are large) into blocks of 'max_block_size' size before insertion into the query result
cache. This reduces performance of writes into the query cache but improves compression rate of cache entries and provides more natural
block granularity when query results are later served from the query cache.

As a result, the query cache stores for each query multiple (partial)
result blocks. While this behavior is a good default, it can be suppressed using setting
[query_cache_squash_partial_results](settings/settings.md#query-cache-squash-partial-results).

Also, results of queries with non-deterministic functions are not cached by default. Such functions include
- functions for accessing dictionaries: [`dictGet()`](../sql-reference/functions/ext-dict-functions.md#dictGet) etc.
- [user-defined functions](../sql-reference/statements/create/function.md),
- functions which return the current date or time: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) etc.,
- functions which return random values: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) etc.,
- functions whose result depends on the size and order or the internal chunks used for query processing:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) etc.,
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) etc.,
- functions which depend on the environment: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](../sql-reference/functions/other-functions.md#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) etc.

To force caching of results of queries with non-deterministic functions regardless, use setting
[query_cache_nondeterministic_function_handling](settings/settings.md#query-cache-nondeterministic-function-handling).

Results of queries that involve system tables, e.g. `system.processes` or `information_schema.tables`, are not cached by default. To force
caching of results of queries with system tables regardless, use setting
[query_cache_system_table_handling](settings/settings.md#query-cache-system-table-handling).

:::note
Prior to ClickHouse v23.11, setting 'query_cache_store_results_of_queries_with_nondeterministic_functions = 0 / 1' controlled whether
results of queries with non-deterministic results were cached. In newer ClickHouse versions, this setting is obsolete and has no effect.
:::

Finally, entries in the query cache are not shared between users due to security reasons. For example, user A must not be able to bypass a
row policy on a table by running the same query as another user B for whom no such policy exists. However, if necessary, cache entries can
be marked accessible by other users (i.e. shared) by supplying setting
[query_cache_share_between_users](settings/settings.md#query-cache-share-between-users).

## Related Content

- Blog: [Introducing the ClickHouse Query Cache](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)
