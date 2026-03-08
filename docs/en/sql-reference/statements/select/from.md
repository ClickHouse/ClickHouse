---
description: 'Documentation for FROM Clause'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: 'FROM Clause'
doc_type: 'reference'
---

# FROM Clause

The `FROM` clause specifies the source to read data from:

- [Table](../../../engines/table-engines/index.md)
- [Subquery](../../../sql-reference/statements/select/index.md) 
- [Table function](/sql-reference/table-functions)

[JOIN](../../../sql-reference/statements/select/join.md) and [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) clauses may also be used to extend the functionality of the `FROM` clause.

Subquery is another `SELECT` query that may be specified in parenthesis inside `FROM` clause.

The `FROM` can contain multiple data sources, separated by commas, which is equivalent of performing [CROSS JOIN](../../../sql-reference/statements/select/join.md) on them.

`FROM` can optionally appear before a `SELECT` clause. This is a ClickHouse-specific extension of standard SQL which makes `SELECT` statements easier to read. Example:

```sql
FROM table
SELECT *
```

## FINAL Modifier {#final-modifier}

When `FINAL` is specified, ClickHouse fully merges the data before returning the result. This also performs all data transformations that happen during merges for the given table engine.

It is applicable when selecting data from tables using the following table engines:
- `ReplacingMergeTree`
- `SummingMergeTree`
- `AggregatingMergeTree`
- `CollapsingMergeTree`
- `VersionedCollapsingMergeTree`

`SELECT` queries with `FINAL` are executed in parallel. The [max_final_threads](/operations/settings/settings#max_final_threads) setting limits the number of threads used.

### Drawbacks {#drawbacks}

Queries that use `FINAL` execute slightly slower than similar queries that do not use `FINAL` because:

- Data is merged during query execution.
- Queries with `FINAL` may read primary key columns in addition to the columns specified in the query.

`FINAL` requires additional compute and memory resources because the processing that normally would occur at merge time must occur in memory at the time of the query. However, using FINAL is sometimes necessary in order to produce accurate results (as data may not yet be fully merged). It is less expensive than running `OPTIMIZE` to force a merge.

As an alternative to using `FINAL`, it is sometimes possible to use different queries that assume the background processes of the `MergeTree` engine have not yet occurred and deal with it by applying an aggregation (for example, to discard duplicates). If you need to use `FINAL` in your queries in order to get the required results, it is okay to do so but be aware of the additional processing required.

`FINAL` can be applied automatically using [FINAL](../../../operations/settings/settings.md#final) setting to all tables in a query using a session or a user profile.

### Example Usage {#example-usage}

Using the `FINAL` keyword

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

Using `FINAL` as a query-level setting

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

Using `FINAL` as a session-level setting

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

## FINAL BY Modifier {#final-by-modifier}

`FINAL BY` is an extension of the [`FINAL`](#final-modifier) modifier that merges rows at a **coarser granularity** than the table's sorting key. Instead of merging rows that share the exact same sorting key, `FINAL BY` lets you specify monotonic functions of the sorting key columns so that rows whose transformed keys are equal get merged together.

This is only supported for [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree) and [`SummingMergeTree`](/engines/table-engines/mergetree-family/summingmergetree) engines.

### Syntax {#final-by-syntax}

```sql
SELECT ... FROM table FINAL BY expr1[, expr2, ...]
```

The number of expressions in the `FINAL BY` clause must exactly match the number of columns in the table's `ORDER BY` clause. Each `FINAL BY` expression must be either:

- **Identity**: the same expression as the corresponding sorting key column (e.g., `ORDER BY x` → `FINAL BY x`), or
- **A monotonic function** of the corresponding sorting key column that preserves sort order (e.g., `ORDER BY unix_time` → `FINAL BY intDiv(unix_time, 10)`).

When all `FINAL BY` expressions are identity (i.e., they match the sorting key exactly), the behavior is equivalent to plain `FINAL`.

### Use Cases {#final-by-use-cases}

`FINAL BY` is designed for producing **bucketed aggregates** directly from `AggregatingMergeTree` or `SummingMergeTree` tables without a separate `GROUP BY`. This is useful for time-series downsampling, where you want to aggregate fine-grained data into coarser time buckets at query time.

For example, a table with per-second metrics (sorted by `unix_time`) can produce per-minute aggregates with `FINAL BY intDiv(unix_time, 60)` — the merge algorithm groups consecutive rows that fall into the same minute bucket and aggregates them.

### Example: Time-Series Downsampling with AggregatingMergeTree {#final-by-example-aggregating}

```sql
CREATE TABLE metrics
(
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY unix_time;

-- Insert per-second data across multiple parts
INSERT INTO metrics
    SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO metrics
    SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

-- Query: aggregate into 10-second buckets
SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM metrics FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
LIMIT 5;
```

```text
┌─bucket─┬─total─┐
│      0 │    30 │
│      1 │    30 │
│      2 │    30 │
│      3 │    30 │
│      4 │    30 │
└────────┴───────┘
```

Each bucket contains 10 rows (values 0–9, 10–19, etc.), each appearing twice with `sumState(1)` and `sumState(2)`, giving `10 × 3 = 30`.

### Example: SummingMergeTree {#final-by-example-summing}

```sql
CREATE TABLE counters
(
    unix_time UInt64,
    val UInt64
)
ENGINE = SummingMergeTree
ORDER BY unix_time;

INSERT INTO counters SELECT number, 1 FROM numbers(100);
INSERT INTO counters SELECT number, 2 FROM numbers(100);

SELECT
    intDiv(unix_time, 10) AS bucket,
    val AS total
FROM counters FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
LIMIT 5;
```

```text
┌─bucket─┬─total─┐
│      0 │    30 │
│      1 │    30 │
│      2 │    30 │
│      3 │    30 │
│      4 │    30 │
└────────┴───────┘
```

### Example: Composite Sorting Key {#final-by-example-composite}

When the table has a composite sorting key, every column must be covered:

```sql
CREATE TABLE trades
(
    token String,
    pair String,
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (token, pair, unix_time);

-- FINAL BY keeps the first two columns as identity, coarsens the last one
SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM trades FINAL BY token, pair, intDiv(unix_time, 10)
PREWHERE token = 'BTC' AND pair = 'USD'
ORDER BY bucket ASC;
```

### Example: Monotonic Function of a Sorting Key Expression {#final-by-example-expression}

If the sorting key itself is an expression, `FINAL BY` can apply a monotonic function on top:

```sql
CREATE TABLE daily_stats
(
    ts DateTime,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY toDate(ts);

-- toStartOfMonth(toDate(ts)) is monotonic over toDate(ts)
SELECT
    toStartOfMonth(toDate(ts)) AS month,
    finalizeAggregation(val) AS total
FROM daily_stats FINAL BY month
ORDER BY month ASC;
```

### Restrictions {#final-by-restrictions}

- **Engine support**: Only `AggregatingMergeTree` and `SummingMergeTree`. Other engines (e.g., `ReplacingMergeTree`, `CollapsingMergeTree`) will throw a `BAD_ARGUMENTS` error.
- **Expression count**: The number of `FINAL BY` expressions must exactly match the number of sorting key columns. Omitting tail columns or adding extra ones is not allowed.
- **Monotonicity**: Each `FINAL BY` expression must be a monotonic function of the corresponding sorting key column that preserves sort order. Direction-reversing functions (e.g., `negate`) are rejected. Expressions that reference multiple sorting key columns (e.g., `a + b`) are also rejected.
## Implementation Details {#implementation-details}

If the `FROM` clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.
