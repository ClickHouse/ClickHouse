---
toc_title: WITH
---

# WITH Clause {#with-clause}

ClickHouse supports Common Table Expressions ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), that is provides to use results of `WITH` clause in the rest of `SELECT` query. Named subqueries can be included to the current and child query context in places where table objects are allowed. Recursion is prevented by hiding the current level CTEs from the WITH expression.

## Syntax

``` sql
WITH <expression> AS <identifier>
```
or
``` sql
WITH <identifier> AS <subquery expression>
```

## Examples {#examples}

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

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/select/with/) <!--hide-->
