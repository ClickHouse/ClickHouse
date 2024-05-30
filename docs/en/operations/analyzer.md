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

**Example:**

The following query uses column `number` in the projection list when only `toString(number)` is available after the aggregation.
In the old infrastructure, `GROUP BY toString(number)` was optimized into `GROUP BY number,` making the query valid.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

### Known incompatibilities for JOIN clause

* Using expression from `SELECT` list in `JOIN` key as an expression from LEFT table. Example.  Fix (best effort, should be under compatibility flag).
* Similar issue ^. Alias for column (in select list) now applied to JOIN result (and not to left table). Example from Denny Crane. New behavior is the correct one. Will try to add best-effort compatibility setting.
* Columns names are changed for some queries. This might breaks some scripts.  Example.


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

Query id: 2a5e39a3-3b64-49fd-bad3-0e351931ac99

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
