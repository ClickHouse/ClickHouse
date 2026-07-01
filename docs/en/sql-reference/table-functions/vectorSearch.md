---
description: 'Returns the K approximate nearest neighbors of a reference vector from a MergeTree table''s vector_similarity index.'
sidebar_label: 'vectorSearch'
sidebar_position: 77
slug: /sql-reference/table-functions/vectorSearch
title: 'vectorSearch'
doc_type: 'reference'
---

# vectorSearch {#vector-search}

Returns the K approximate nearest neighbors of a reference vector from a `MergeTree` table's `vector_similarity` index. The search is approximate: it queries the persisted index and has no exact-search fallback, so the result may differ from the exact K nearest neighbors (see [Approximate vector search](../../engines/table-engines/mergetree-family/annindexes.md#approximate-nearest-neighbor-search)).

:::note
The table function is experimental. To enable it, set [`allow_experimental_search_topk_table_functions`](../../operations/settings/settings.md) to `1`. See [Experimental gate](#experimental-gate) below.
:::

## Syntax {#syntax}

Two forms are supported.

The 4-argument form auto-resolves the source table's `vector_similarity` index. The source table must have exactly one such index for this form to be valid:

```sql
vectorSearch(database, table, reference_vector, K)
```

The 5-argument form names the index explicitly, which is required when the source table has more than one `vector_similarity` index:

```sql
vectorSearch(database, table, index_name, reference_vector, K)
```

## Arguments {#arguments}

| Argument           | Description                                                                                  |
|--------------------|----------------------------------------------------------------------------------------------|
| `database`         | The database that owns the source table. `Identifier` or `String` literal.                   |
| `table`            | The source `MergeTree`-family table. `Identifier` or `String` literal.                       |
| `index_name`       | Name of the `vector_similarity` index to search. `Identifier`. Required in the 5-arg form; omitted in the 4-arg form. |
| `reference_vector` | The query vector. `Array(Float32)` literal whose length matches the index's `<dimensions>`.  |
| `K`                | Number of approximate nearest neighbors to return. `UInt64` literal.                         |

## Returned columns {#returned-columns}

The result is a virtual table whose columns are, in this order:

1. All columns of the source table, in their source declaration order.
2. `_score` (`Float32`) — USearch's native distance for the index's metric. See [Sorting](#sorting) for the per-metric semantics.

`_score` carries the index's native distance, so its meaning depends on the index's `<distance_function>`:

| Metric           | `_score` value                                                                                  | Direction       |
|------------------|--------------------------------------------------------------------------------------------------|-----------------|
| `L2Distance`     | raw squared L2 distance                                                                          | `ASC` is closer |
| `cosineDistance` | raw cosine distance `1 - cos(θ)` ∈ `[0, 2]`                                                      | `ASC` is closer |
| `dotProduct`     | `1 - dot(a, b)` — USearch's `metric_ip_gt` already inverts the inner product so lower is closer  | `ASC` is closer |

### Virtual columns {#virtual-columns}

In addition to the returned columns above, `vectorSearch` exposes every [virtual column](../../engines/table-engines/mergetree-family/mergetree.md#virtual-columns) of the source `MergeTree` table — `_part`, `_part_index`, `_part_offset`, `_partition_id`, `_partition_value`, `_block_number`, `_block_offset`, `_part_data_version`, `_disk_name`, `_row_exists`, and so on. Each carries the same value it would have for a plain `SELECT` from the source table; for example `_part` is the source part each row was read from, and `_part_offset` is the row's offset within that part. Like all virtual columns, they are not included in `SELECT *` and must be referenced explicitly, and they can be used in the `WHERE` clause. Most of them drive the prefilter, but predicates on the row-locator virtual columns `_part_index` and `_part_offset` are post-filtered exceptions — see [WHERE-clause prefilter](#where-clause-prefilter).

For compatibility with the legacy distance column name, `_distance` is exposed as an alias for `_score`. Referencing `_distance` (in the select list, `WHERE`, or `ORDER BY`) resolves to `_score`; being an alias, it does not appear in `SELECT *`. If the source table declares its own column named `_distance`, that column wins and no alias is added.

## Sorting {#sorting}

Always use `ORDER BY _score ASC`. Lower is closer for every supported metric, so the same `ORDER BY` clause works regardless of the index's `<distance_function>` — user queries do not need a per-metric branch. `_distance` is accepted as an alias for `_score` (see [Virtual columns](#virtual-columns)).

## WHERE-clause prefilter {#where-clause-prefilter}

`WHERE`-clause predicates that can be evaluated against source-table columns (and source-table virtual columns such as `_part`) are evaluated as a bitmap prefilter before the vector index is queried. The prefilter is built by the same machinery that serves a normal `SELECT [...] WHERE [...] FROM table`, so the following predicates all participate:

- Text-index predicates: `hasAllTokens`, `hasAnyTokens`, `hasPhrase`, `LIKE`.
- Primary-key range conditions.
- Skipping-index conditions (minmax, set, bloom_filter, etc.).
- Partition pruning.

Predicates on scorer-produced columns such as `_score` — and mixed predicates that cannot be split from them, for example an `OR` between a source-column predicate and a `_score` predicate — do not participate in the prefilter. They are evaluated as an ordinary filter after `vectorSearch` has produced its top-K rows, so they can only shrink the result below `K` rows.

The row-locator virtual columns `_part_index` and `_part_offset` are post-filtered exceptions among the source virtual columns. The bitmap subquery reads these two columns internally to map matched rows back to their parts, so a user predicate on them would collide with that internal use and is *not* pushed into the prefilter. Like a `_score` predicate, it is applied as an ordinary filter after the top-K rows have been produced. As a consequence, `WHERE _part_offset = 0` returns the subset of the top-K rows that happen to sit at part-offset `0` — it can return fewer than `K` rows and is not the top-K within that offset subset. Every other source virtual column (`_part`, `_partition_id`, `_block_number`, `_disk_name`, and so on) does participate in the prefilter.

Row policies defined on the source table also participate in the prefilter, so rows hidden by a policy are never considered as candidates.

When a prefilter exists, USearch's `filtered_search` is called and only rows in the bitmap are considered as candidates by the index. This is the mechanism that replaces the previous `mergeTreeHybridSearch` table function — see [Migration from `mergeTreeHybridSearch`](#migration-from-mergetreehybridsearch).

## Example {#example}

```sql
-- 4-arg form — auto-resolves the source table's single
-- vector_similarity index. `_score` is USearch's native
-- distance for every metric (lower is better), so
-- `ORDER BY _score ASC` is universal — no per-metric branch
-- in user queries.
SELECT id, title, _score
FROM vectorSearch(default, docs, [0.1, 0.2 /* ... */]::Array(Float32), 10)
ORDER BY _score ASC
SETTINGS allow_experimental_search_topk_table_functions = 1;
```

```sql
-- WHERE clause drives the prefilter; reaches what the removed
-- mergeTreeHybridSearch table function did via positional args.
SELECT id, title, _score
FROM vectorSearch(default, docs, [0.1, 0.2 /* ... */]::Array(Float32), 10)
WHERE hasAllTokens(body, ['error', 'timeout'])
  AND event_date >= today() - 7
ORDER BY _score ASC
SETTINGS allow_experimental_search_topk_table_functions = 1;
```

## Migration from `mergeTreeHybridSearch` {#migration-from-mergetreehybridsearch}

The previous internal `mergeTreeHybridSearch` table function has been removed. Its positional text-postings argument is replaced by a standard `WHERE` clause on `vectorSearch`:

- `mergeTreeHybridSearch(...)` with an all-tokens text predicate → `vectorSearch(...) WHERE hasAllTokens(col, tokens)`.
- `mergeTreeHybridSearch(...)` with an any-tokens text predicate → `vectorSearch(...) WHERE hasAnyTokens(col, tokens)`.

The new form composes with any other `WHERE` predicate (PK ranges, skip indexes, partition keys), so a single `vectorSearch` call covers a strictly wider surface than the removed function.

## Experimental gate {#experimental-gate}

The table function is gated by the setting `allow_experimental_search_topk_table_functions`, which defaults to `false`. Enable it per query:

```sql
SET allow_experimental_search_topk_table_functions = 1;
```

or as a `SETTINGS` clause on the query itself, as shown in [Example](#example).

## Limitations {#limitations}

- The source table must have a `vector_similarity` index. See [Approximate vector search](../../engines/table-engines/mergetree-family/annindexes.md#approximate-nearest-neighbor-search) for how to create one.
- The source table must belong to the `MergeTree` family. `Distributed` tables are not accepted directly; use [`remote`](remote.md) (or `clusterAllReplicas`) to fan the table function out across replicas.
- The 4-argument form requires exactly one `vector_similarity` index on the source table. If the table has zero or more than one such index, use the 5-argument form and name the index explicitly.
- Each selected part must carry a single `vector_similarity` sub-index, i.e. at most one index granule per part; parts with more than one granule are rejected. This holds with the default index `GRANULARITY` (100 million) for parts of up to 100 million rows. Smaller explicit `GRANULARITY` values, as discussed in [Approximate vector search](../../engines/table-engines/mergetree-family/annindexes.md#approximate-nearest-neighbor-search), are not supported by `vectorSearch` yet.
- `K` must not exceed the setting [`max_limit_for_vector_search_queries`](../../operations/settings/settings.md#max_limit_for_vector_search_queries). The legacy `ORDER BY ... LIMIT` surface falls back to an exact search above the cap; `vectorSearch` has no exact-search fallback, so it rejects the query instead.
- The index must be present in every selected part. After `ALTER TABLE ... ADD INDEX`, run `ALTER TABLE ... MATERIALIZE INDEX` to build the index for existing parts — otherwise the query is rejected.
- Parts whose indexed columns have pending on-the-fly updates (not-yet-applied `ALTER TABLE ... UPDATE` or lightweight updates) are rejected, because the persisted index does not reflect the updated values yet.
- Rows hidden by lightweight deletes are excluded through the bitmap prefilter. When the query has no `WHERE` clause and a part contains deleted rows, an implicit `_row_exists` prefilter is built, so such queries are subject to [`search_topk_prefilter_max_rows`](../../operations/settings/settings.md#search_topk_prefilter_max_rows) like any prefiltered query.
- The legacy `SELECT [...] ORDER BY <DistanceFunction>(vectors, reference_vector) LIMIT N` SQL surface described in [Approximate vector search](../../engines/table-engines/mergetree-family/annindexes.md#approximate-nearest-neighbor-search) continues to work unchanged and remains the recommended surface for queries that do not need an explicit `_score` projection.
