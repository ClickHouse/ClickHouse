---
slug: /en/operations/analyzer
sidebar_label: Analyzer
title: Analyzer
description: Details about ClickHouse's query analyzer
keywords: [analyzer]
---

# Analyzer

<BetaBadge />

## Known incompatibilities

In ClickHouse version `24.3`, the new query analyzer was enabled by default.
Despite fixing a large number of bugs and introducing new optimizations, it also introduces some breaking changes in ClickHouse behaviour. Please read the following changes to determine how to rewrite your queries for the new analyzer.

### Invalid queries are no longer optimized

The previous query planning infrastructure applied AST-level optimizations before the query validation step.
Optimizations could rewrite the initial query so it becomes valid and can be executed.

In the new analyzer, query validation takes place before the optimization step.
This means that invalid queries that were possible to execute before are now unsupported.
In such cases, the query must be fixed manually.

**Example 1:**

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

The following query uses column `number` in the projection list when only `toString(number)` is available after the aggregation.
In the old analyzer, `GROUP BY toString(number)` was optimized into `GROUP BY number,` making the query valid.

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

To fix the query, you should move all conditions that apply to non-aggregated columns to the `WHERE` section to conform to standard SQL syntax:
```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

### CREATE VIEW with invalid query

The new analyzer always performs type-checking.
Previously, it was possible to create a `VIEW` with an invalid `SELECT` query. It would then fail during the first `SELECT` or `INSERT` (in the case of `MATERIALIZED VIEW`).

Now, it's not possible to create such `VIEW`s anymore.

**Example:**

```sql
CREATE TABLE source (data String) ENGINE=MergeTree ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

### Known incompatibilities of the `JOIN` clause

#### Join using column from projection

Alias from the `SELECT` list can not be used as a `JOIN USING` key by default.

A new setting, `analyzer_compatibility_join_using_top_level_identifier`, when enabled, alters the behavior of `JOIN USING` to prefer to resolve identifiers based on expressions from the projection list of the `SELECT` query, rather than using the columns from left table directly.

**Example:**

```sql
SELECT a + 1 AS b, t2.s
FROM Values('a UInt64, b UInt64', (1, 1)) AS t1
JOIN Values('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

With `analyzer_compatibility_join_using_top_level_identifier` set to `true`, the join condition is interpreted as `t1.a + 1 = t2.b`, matching the behavior of earlier versions. So, the result will be `2, 'two'`.
When the setting is `false`, the join condition defaults to `t1.b = t2.b`, and the query will return `2, 'one'`.
If `b` is not present in `t1`, the query will fail with an error.

#### Changes in behavior with `JOIN USING` and `ALIAS`/`MATERIALIZED` columns

In the new analyzer, using `*` in a `JOIN USING` query that involves `ALIAS` or `MATERIALIZED` columns will include those columns in the result set by default.

**Example:**

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

#### Handling of Type Modifiers for columns in `USING` Clause

In the new version of the analyzer, the rules for determining the common supertype for columns specified in the `USING` clause have been standardized to produce more predictable outcomes, especially when dealing with type modifiers like `LowCardinality` and `Nullable`.

- `LowCardinality(T)` and `T`: When a column of type `LowCardinality(T)` is joined with a column of type `T`, the resulting common supertype will be `T`, effectively discarding the `LowCardinality` modifier.

- `Nullable(T)` and `T`: When a column of type `Nullable(T)` is joined with a column of type `T`, the resulting common supertype will be `Nullable(T)`, ensuring that the nullable property is preserved.

**Example:**

```sql
SELECT id, toTypeName(id) FROM Values('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN Values('id String', ('b')) AS t2
USING (id);
```

In this query, the common supertype for `id` is determined as `String`, discarding the `LowCardinality` modifier from `t1`.

### Projection column names changes

During projection names computation, aliases are not substituted.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

### Incompatible function arguments types

In the new analyzer, type inference happens during initial query analysis.
This change means that type checks are done before short-circuit evaluation; thus, `if` function arguments must always have a common supertype.

**Example:**

The following query fails with `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

### Heterogeneous clusters

The new analyzer significantly changed the communication protocol between servers in the cluster. Thus, it's impossible to run distributed queries on servers with different `enable_analyzer` setting values.

### Mutations are interpreted by previous analyzer

Mutations are still using the old analyzer.
This means some new ClickHouse SQL features can't be used in mutations. For example, the `QUALIFY` clause.
Status can be checked [here](https://github.com/ClickHouse/ClickHouse/issues/61563).

### Unsupported features

The list of features new analyzer currently doesn't support:

- Annoy index.
- Hypothesis index. Work in progress [here](https://github.com/ClickHouse/ClickHouse/pull/48381).
- Window view is not supported. There are no plans to support it in the future.
