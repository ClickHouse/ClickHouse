---
slug: /en/sql-reference/statements/select/with
sidebar_label: WITH
---

# WITH Clause

ClickHouse supports Common Table Expressions ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)). CTEs can be non-materialized or materialized.

Non-materialized CTEs substitute the code defined in the `WITH` clause in all places of use for the rest of `SELECT` query. Named subqueries can be included to the current and child query context in places where table objects are allowed. Recursion is prevented by hiding the current level CTEs from the WITH expression. Please note that non-materialized CTEs do not guarantee the same results in all places they are called because the query will be re-executed for each use case. An example of such behavior is below

``` sql
with cte_numbers as
(
    select
        num
    from generateRandom('num UInt64', NULL)
    limit 1000000
)
select
    count()
from cte_numbers
where num in (select num from cte_numbers)
```
If CTEs were to pass exactly the results and not just a piece of code, you would always see `1000000`

However, due to the fact that we are referring `cte_numbers` twice, random numbers are generated each time and, accordingly, we see different random results, `280501, 392454, 261636, 196227` and so on...

Materialized CTEs are executed only once and the result is stored in memory. The result is then used in all places of use for the rest of the `SELECT` query. Materialized CTEs are useful when you want to avoid re-executing the same subquery multiple times. By default, a materialized CTE table has engine `Memory`, but you can specify another engine (`Join` or `Set`) in the `WITH` clause.

Materialized CTEs has no scope, they are visible in the whole query, including the subqueries and nested queries. If a materialized CTE is defined in a subquery, it will also be visible in the parent query and other subqueries. As a result, you cannot have two materialized CTEs with the same name in the same query.

By default, temporary tables created by materialized CTEs are not sent to remote shards when executing distributed queries. You can change this behavior by setting the `send_materialized_cte_tables_to_remote_shard` to `true`.

## Syntax

``` sql
WITH <expression> AS <identifier>
```
or
``` sql
WITH <identifier> AS [MATERIALIZED]<subquery expression> [ENGINE = Memory|Join(...)|Set]
```

## Examples

**Example 1:** Using constant expression as “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**Example 2:** Evicting a sum(bytes) expression result from the SELECT clause column list

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**Example 3:** Using results of a scalar subquery

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
LIMIT 10;
```

**Example 4:** Reusing expression in a subquery

``` sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

**Example 5:** Using a materialized CTE to create a temporary hash table and use it in multiple joins

``` sql
WITH dict AS MATERIALIZED
(
    SELECT
        number AS key,
        toString(number) AS value
    FROM numbers(100)
) Join(ANY, LEFT, key)
SELECT *
FROM t
ANY LEFT JOIN dict AS d1 ON k1 = d1.k
ANY LEFT JOIN dict AS d2 ON k2 = d2.k
```
