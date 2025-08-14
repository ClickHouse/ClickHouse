---
description: 'Guide to using and configuring the query condition cache feature in ClickHouse'
sidebar_label: 'Query Condition Cache'
sidebar_position: 64
slug: /operations/query-condition-cache
title: 'Query Condition Cache'
---

# Query Condition Cache

Many real-world workloads involve repeated queries against the same or almost the same data (for instance, previously existing data plus new data).
ClickHouse provides various optimization techniques to optimize for such query patterns.
One possibility is to tune the physical data layout using index structures (e.g., primary key indexes, skipping indexes, projections) or pre-calculation (materialized views).
Another possibility is to use ClickHouse's [query cache](query-cache.md) to avoid repeated query evaluation.
The downside of the first approach is that that it requires manual intervention and monitoring by a database administrator.
The second approach may return stale results (as the query cache is transactionally not consistent) which may or may not be acceptable, depending on the use case.

The query condition cache provides an elegant solution for both problems.
It is based on the idea that evaluating a filter condition (e.g., `WHERE col = 'xyz'`) on the same data will always return the same results.
More specifically, the query condition cache remembers for each each evaluated filter and each granule (= a block of 8192 rows by default) if no row in the granule satisfy the filter condition.
The information is recorded as a single bit: a 0 bit represents that no row matches the filter whereas a 1 bit means that at least one matching row exists.
In the former case, ClickHouse may skip the corresponding granule during filter evaluation, in the latter case, the granule must be loaded and evaluated.

The query condition cache is effective if three prerequisites are fulfilled:
- First, the workload must evaluate the same filter conditions repeatedly. This happens naturally if a query is repeated multiple times but it can also happen if two queries share the same filters, e.g. `SELECT product FROM products WHERE quality > 3` and `SELECT vendor, count() FROM products WHERE quality > 3`.
- Second, the majority of the data is immutable, i.e., does not change between queries. This is generally the case in ClickHouse as parts are immutable and created only by INSERTs.
- Third, filters are selective, i.e. only relatively few rows satisfy the filter condition. The fewer rows match the filter condition, the more granules will be recorded with bit 0 (no matching rows), and the more data can be "pruned" from subsequent filter evaluations.

## Memory Consumption {#memory-consumption}

Since the query condition cache stores only a single bit per filter condition and granule, it consumes only little memory.
The maximum size of the query condition cache can be configured using server settings [`query_condition_cache_size`](server-configuration-parameters/settings.md#query_condition_cache_size) (default: 100 MB).
A cache size of 100 MB corresponds to 100 * 1024 * 1024 * 8 = 838,860,800 entries.
Since each entry represents a mark (8192 rows by default), the cache can cover up to 6,871,947,673,600 (6.8 trillion) rows of a single column.
In practice, filter are evaluated on more than one column, so that number needs to be divided by the number of filtered columns.

## Configuration Settings and Usage {#configuration-settings-and-usage}

Setting [use_query_condition_cache](settings/settings#use_query_condition_cache) controls whether a specific query or all queries of the current session should utilize the query condition cache.

For example, the first execution of query

```sql
SELECT col1, col2
FROM table
WHERE col1 = 'x'
SETTINGS use_query_condition_cache = true;
```

will store ranges of the table which do not satisfy the predicate.
Subsequent executions of the same query, also with parameter `use_query_condition_cache = true`, will utilize the query condition cache to scan less data.

:::note
The query condition cache only works when [allow_experimental_analyzer](https://clickhouse.com/docs/operations/settings/settings#allow_experimental_analyzer) is set to true, which is the default value.
:::

## Administration {#administration}

The query condition cache is not retained between restarts of ClickHouse.

To clear the query condition cache, run [`SYSTEM DROP QUERY CONDITION CACHE`](../sql-reference/statements/system.md#drop-query-condition-cache).

The content of the cache is displayed in system table [system.query_condition_cache](system-tables/query_condition_cache.md).
To calculate the current size of the query condition cache in MB, run `SELECT formatReadableSize(sum(entry_size)) FROM system.query_condition_cache`.
If you like to investigate individual filter conditions, you can check field `condition` in `system.query_condition_cache`.
Note that the field is only populated if the query runs with enabled setting [query_condition_cache_store_conditions_as_plaintext](settings/settings#query_condition_cache_store_conditions_as_plaintext).

The number of query condition cache hits and misses since database start are shown as events "QueryConditionCacheHits" and "QueryConditionCacheMisses" in system table [system.events](system-tables/events.md).
Both counters are only updated for `SELECT` queries which run with setting `use_query_condition_cache = true`, other queries do not affect "QueryCacheMisses".

## Related Content {#related-content}

- Blog: [Introducing the Query Condition Cache](https://clickhouse.com/blog/introducing-the-clickhouse-query-condition-cache)
- [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses (Schmidt et. al., 2024)](https://doi.org/10.1145/3626246.3653395)
