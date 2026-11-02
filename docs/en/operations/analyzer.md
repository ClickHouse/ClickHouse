---
description: 'Page detailing the ClickHouse query analyzer'
keywords: ['analyzer']
sidebar_label: 'Analyzer'
slug: /operations/analyzer
title: 'Analyzer'
doc_type: 'reference'
---

# Analyzer

In ClickHouse version `24.3`, the new query analyzer was enabled by default.
You can read more details about how it works [here](/guides/developer/understanding-query-execution-with-the-analyzer#analyzer).

## Known incompatibilities {#known-incompatibilities}

Despite fixing a large number of bugs and introducing new optimizations, it also introduces some breaking changes in ClickHouse behaviour. Please read the following changes to determine how to rewrite your queries for the analyzer.

### Invalid queries are no longer optimized {#invalid-queries-are-no-longer-optimized}

The previous query planning infrastructure applied AST-level optimizations before the query validation step.
Optimizations could rewrite the initial query to be valid and executable.

In the analyzer, query validation takes place before the optimization step.
This means that invalid queries which were previously possible to execute, are now unsupported.
In such cases, the query must be fixed manually.

#### Example 1 {#example-1}

The following query uses column `number` in the projection list when only `toString(number)` is available after the aggregation.
In the old analyzer, `GROUP BY toString(number)` was optimized into `GROUP BY number,` making the query valid.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

#### Example 2 {#example-2}

The same problem occurs in this query. Column `number` is used after aggregation with another key.
The previous query analyzer fixed this query by moving the `number > 5` filter from the `HAVING` clause to the `WHERE` clause.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

To fix the query, you should move all conditions that apply to non-aggregated columns to the `WHERE` section to conform to standard SQL syntax:

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

### `CREATE VIEW` with an invalid query {#create-view-with-invalid-query}

The analyzer always performs type-checking.
Previously, it was possible to create a `VIEW` with an invalid `SELECT` query.
It would then fail during the first `SELECT` or `INSERT` (in the case of `MATERIALIZED VIEW`).

It is no longer possible to create a `VIEW` in this way.

#### Example {#example-view}

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

### Known incompatibilities of the `JOIN` clause {#known-incompatibilities-of-the-join-clause}

#### `JOIN` using a column from a projection {#join-using-column-from-projection}

An alias from the `SELECT` list can not be used as a `JOIN USING` key by default.

A new setting, `analyzer_compatibility_join_using_top_level_identifier`, when enabled, alters the behavior of `JOIN USING` to prefer resolving identifiers based on expressions from the projection list of the `SELECT` query, rather than using the columns from the left table directly.

For example:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

With `analyzer_compatibility_join_using_top_level_identifier` set to `true`, the join condition is interpreted as `t1.a + 1 = t2.b`, matching the behavior of the earlier versions.
The result will be `2, 'two'`.
When the setting is `false`, the join condition defaults to `t1.b = t2.b`, and the query will return `2, 'one'`.
If `b` is not present in `t1`, the query will fail with an error.

#### Changes in behavior with `JOIN USING` and `ALIAS`/`MATERIALIZED` columns {#changes-in-behavior-with-join-using-and-aliasmaterialized-columns}

In the analyzer, using `*` in a `JOIN USING` query that involves `ALIAS` or `MATERIALIZED` columns will include those columns in the result-set by default.

For example:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

In the analyzer, the result of this query will include the `payload` column along with `id` from both tables.
In contrast, the previous analyzer would only include these `ALIAS` columns if specific settings (`asterisk_include_alias_columns` or `asterisk_include_materialized_columns`) were enabled,
and the columns might appear in a different order.

To ensure consistent and expected results, especially when migrating old queries to the analyzer, it is advisable to specify columns explicitly in the `SELECT` clause rather than using `*`.

#### Handling of type modifiers for columns in the `USING` clause {#handling-of-type-modifiers-for-columns-in-using-clause}

In the new version of the analyzer, the rules for determining the common supertype for columns specified in the `USING` clause have been standardized to produce more predictable outcomes,
especially when dealing with type modifiers like `LowCardinality` and `Nullable`.

- `LowCardinality(T)` and `T`: When a column of type `LowCardinality(T)` is joined with a column of type `T`, the resulting common supertype will be `T`, effectively discarding the `LowCardinality` modifier.
- `Nullable(T)` and `T`: When a column of type `Nullable(T)` is joined with a column of type `T`, the resulting common supertype will be `Nullable(T)`, ensuring that the nullable property is preserved.

For example:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

In this query, the common supertype for `id` is determined as `String`, discarding the `LowCardinality` modifier from `t1`.

### Projection column names changes {#projection-column-names-changes}

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

### Incompatible function arguments types {#incompatible-function-arguments-types}

In the analyzer, type inference happens during initial query analysis.
This change means that type checks are done before short-circuit evaluation; thus, the `if` function arguments must always have a common supertype.

For example, the following query fails with `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

### Heterogeneous clusters {#heterogeneous-clusters}

The analyzer significantly changes the communication protocol between servers in the cluster. Thus, it's impossible to run distributed queries on servers with different `enable_analyzer` setting values.

### Mutations are interpreted by previous analyzer {#mutations-are-interpreted-by-previous-analyzer}

Mutations are still using the old analyzer.
This means some new ClickHouse SQL features can't be used in mutations. For example, the `QUALIFY` clause.
The status can be checked [here](https://github.com/ClickHouse/ClickHouse/issues/61563).

### Unsupported features {#unsupported-features}

The list of features that the analyzer currently doesn't support is given below:

- Annoy index.
- Hypothesis index. Work in progress [here](https://github.com/ClickHouse/ClickHouse/pull/48381).
- Window view is not supported. There are no plans to support it in the future.

## Cloud Migration {#cloud-migration}

We are enabling the new query analyzer on all instances where it is currently disabled to support new functional and performance optimizations. This change enforces stricter SQL scoping rules, requiring customers to manually update non-compliant queries.

### Migration workflow {#migration-workflow}

1. Identify the query by filtering `system.query_log` using the `normalized_query_hash`:
```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. Run the query with the analyzer enabled by adding these settings.
```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. Refactor and verify the query results to ensure they match the output generated when the analyzer is disabled.   

Please refer to the most frequent incompatibilities encountered during internal testing.

### Unknown expression identifier {#unknown-expression-identifier}

Error: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. Exception code: 47

Cause: Queries that rely on non-standard, permissive legacy behaviors such as referencing calculated aliases in filters, ambiguous subquery projections, or "dynamic" CTE scoping are now correctly identified as invalid and rejected immediately.   

Solution: Update your SQL patterns as follows:
- Filter logic: Move logic from WHERE to HAVING if filtering on results, or duplicate the expression in WHERE if filtering on source data.
- Subquery scope: Explicitly select all columns needed by the outer query.
- JOIN keys: Use ON with full expressions instead of USING if the key is an alias.
- In outer queries, refer to the alias of the Subquery/CTE itself, not the tables inside it.

### Non-Aggregated Columns in GROUP BY {#non-aggregated-columns-in-group-by}

Error: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. Exception code: 215 

Cause: The old analyzer allowed selecting columns not present in the GROUP BY clause (often picking an arbitrary value). The analyzer adheres to standard SQL: every selected column must be either an aggregate or a grouping key. 

Solution: Wrap the column in `any()`, `argMax()`, or add it to the GROUP BY.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

### Duplicate CTE names {#duplicate-cte-names}

Error: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Exception code: 179

Cause: The old analyzer permitted defining multiple Common Table Expressions (WITH ...) with the same name shadowing the earlier one. The analyzer forbids this ambiguity. 

Solution: Rename duplicate CTEs to be unique.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

### Ambiguous column identifiers {#ambiguous-column-identifiers}

Error: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` Exception code: 207

Cause: The query references a column name present in multiple tables within a JOIN without specifying the source table. The old analyzer often guessed the column based on internal logic, the analyzer requires explicit name. 

Solution: Fully qualify the column with table_alias.column_name.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

### Invalid usage of FINAL {#invalid-usage-of-final}

Error: `Table expression modifiers FINAL are not supported for subquery...` or `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). Exception codes: 1, 181 

Cause: FINAL is a modifier for table storage (specifically [Shared]ReplacingMergeTree). The analyzer rejects FINAL when applied to:
- Subqueries or derived tables (e.g., FROM (SELECT ...) FINAL).
- Table engines that do not support it (e.g., SharedMergeTree). 

Solution: Apply FINAL only to the source table inside the subquery, or remove it if the engine does not support it.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

### `countDistinct()` function case-insensitivity {#countdistinct-case-insensitivity}

Error: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. Exception code: 46

Cause: Function names are case-sensitive or strictly mapped in the analyzer. `countdistinct` (all lowercase) is no longer resolved automatically. 

Solution: Use the standard `countDistinct` (camelCase) or the ClickHouse specific uniq.
