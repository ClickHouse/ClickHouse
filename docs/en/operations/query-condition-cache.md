---
slug: /operations/query-cache
sidebar_position: 64
sidebar_label: Query Condition Cache
---

# Query Condition Cache

Many practical workloads expose repeated queries against the same data.
If users are aware of these patterns, they can create primary key indexes, skipping indexes, projections etc., to speed up such queries.
This however requires careful monitoring of the workload and explicit tuning (i.e., physical schema optimization) from a database administrator.

The query condition cache addresses this by remembering which ranges of the data cannot possibly satisfy filter predicates.
For example, if the user runs the query `SELECT * FROM tab WHERE col = 'a'`, ClickHouse will scan column `col` for filter predicate `col = 'a'`.
At the end of the scan, the query condition cache will remember which vertical ranges of table `tab` did not match the predicate.
When the user runs the same query again, ClickHouse will first check the cache and exclude non-matching data ranges from the subsequent scan.

## Configuration Settings and Usage {#configuration-settings-and-usage}

Setting [use_query_cache](/operations/settings/settings#use_query_condition_cache) controls whether a specific query or all queries of the
current session should utilize the query condition cache. For example, the first execution of query

```sql
SELECT col1, col2
FROM table
WHERE col1 = 'x'
SETTINGS use_query_condition_cache = true;
```

will store ranges of the table which do not satisfy the predicate.
Subsequent executions of the same query (also with parameter `use_query_condition_cache = true`) will utilize the query condition cache to scan less data.
