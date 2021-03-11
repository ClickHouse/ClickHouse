---
toc_title: WITH
---

# WITH Clause {#with-clause}

This section provides support for Common Table Expressions ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), so the results of `WITH` clause can be used in the rest of `SELECT` query.

## Limitations {#limitations}

1.  Recursive queries are not supported.
2.  When subquery is used inside WITH section, it’s result should be scalar with exactly one row.
3.  Expression’s results are not available in subqueries.

## Examples {#examples}

**Example 1:** Using constant expression as “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

**Example 2:** Evicting sum(bytes) expression result from SELECT clause column list

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

**Example 3:** Using results of scalar subquery

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

**Example 4:** Re-using expression in subquery

As a workaround for current limitation for expression usage in subqueries, you may duplicate it.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```
