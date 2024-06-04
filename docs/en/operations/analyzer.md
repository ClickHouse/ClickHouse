---
slug: /en/operations/analyzer
sidebar_label: Analyzer
---

# Analyzer

<BetaBadge />

## Known incompatibilities

In ClickHouse version `24.3`, new query analysis was enabled by default.
Despite fixing a large number of bugs and introducing new optimizations, it also introduces some breaking changes in ClickHouse behaviour.

### Invalid before optimization queries

The previous query planning infrastructure applied AST-level optimizations before the query validation step.
Optimizations could rewrite the initial query so it becomes valid and can be executed.

In the new infrastructure, query validation takes place before the optimization step.
This means that invalid queries that were possible to execute before are now unsupported.

**Example 1:**

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

The following query uses column `number` in the projection list when only `toString(number)` is available after the aggregation.
In the old infrastructure, `GROUP BY toString(number)` was optimized into `GROUP BY number,` making the query valid.

**Example 2:**

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

The same problem occurs in this query: column `number` is used after aggregation with another key.
The previous query analyzer fixed this query by moving the `number > 5` filter from the `HAVING` clause to the `WHERE` clause.

### Known incompatibilities of JOIN clause

#### Join using column from projection

Alias from the `SELECT` list can not be used as a `JOIN USING` key  by default.

A new setting, `analyzer_compatibility_join_using_top_level_identifier`, when enabled, alters the behavior of `JOIN USING` to prefer to resolve identifiers based on expressions from the projection list of the SELECT query, rather than using the columns from left table directly.

*Example:*

```sql
SELECT a + 1 AS b, t2.s
FROM Values('a UInt64, b UInt64', (1, 1)) AS t1
JOIN Values('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

With `analyzer_compatibility_join_using_top_level_identifier` set to `true`, the join condition is interpreted as `t1.a + 1 = t2.b`, matching the behavior of earlier versions. So, the result will be `2, 'two'`
When the setting is `false`, the join condition defaults to `t1.b = t2.b`, and the query will return `2, 'one'`.
In case then `b` is not present in `t1`, the query will fail with an error.

#### Changes in Behavior with `JOIN USING` and `ALIAS/MATERIALIZED` Columns

In the new analyzer, using `*` in a `JOIN USING` query that involves `ALIAS` or `MATERIALIZED` columns will include that columns in the result set by default.

*Example:*

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

In the new analyzer, the result of this query will include the `payload` column along with `id` from both tables. In contrast, the previous analyzer would only include these `ALIAS` columns if specific settings (`asterisk_include_alias_columns` or `asterisk_include_materialized_columns`) were enabled, and the columns might appear in a different order.

To ensure consistent and expected results, especially when migrating old queries to the new analyzer, it is advisable to specify columns explicitly in the `SELECT` clause rather than using `*`.


### Projection column names changes

During projection names computation aliases are not substituted.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS allow_experimental_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS allow_experimental_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

### Incompatible function arguments types

In the new infrastructure type inference happens during initial query analysis.
This change means that type checks are done before short-circuit evaluation; thus, `if` function arguments must always have a common supertype.

**Example:**

The following query fails with `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

### Heterogeneous clusters

The new analyzer significantly changed the communication protocol between servers in the cluster. Thus, it's impossible to run distributed queries on servers with different `allow_experimental_analyzer` setting values.

### Unsupported features

The list of features new analyzer currently doesn't support:

- Annoy index.
- Hypothesis index. Work in progress [here](https://github.com/ClickHouse/ClickHouse/pull/48381).
- Window view is not supported. There are no plans to support it in the future.
