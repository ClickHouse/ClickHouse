---
description: 'Documentation for Exact and Approximate Nearest Neighbor Search'
keywords: ['vector similarity search', 'ann', 'knn', 'hnsw', 'indices', 'index', 'nearest neighbor']
sidebar_label: 'Exact and Approximate Nearest Neighbor Search'
slug: /engines/table-engines/mergetree-family/annindexes
title: 'Exact and Approximate Nearest Neighbor Search'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import PrivatePreviewBadge from '@theme/badges/PrivatePreviewBadge';

# Exact and Approximate Nearest Neighbor Search

<ExperimentalBadge/>
<PrivatePreviewBadge/>

The problem of finding the N closest points in a multi-dimensional (vector) space for a given point is known as [nearest neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search).
Two general approaches exist for solving nearest neighbor search:
- Exact nearest neighbor search calculates the distance between the given point and all points in the vector space. This ensures the best possible accuracy, i.e. the returned points are guaranteed to be the actual nearest neighbors. Since the vector space is explored exhaustively, exact nearest neighbor search can be too slow for real-world use.
- Approximate nearest neighbor search refers to a group of techniques (e.g., special data structures like graphs and random forests) which compute results much faster than exact nearest neighbor search. The result accuracy is typically "good enough" for practical use. Many approximate techniques provide parameters to tune the trade-off between the result accuracy and the search time.

A nearest neighbor search (exact or approximate) can be written in SQL as follows:

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

The points in the vector space are stored in a column `vectors` of array type, e.g. [Array(Float64)](../../../sql-reference/data-types/array.md), [Array(Float32)](../../../sql-reference/data-types/array.md), or [Array(BFloat16)](../../../sql-reference/data-types/array.md).
The reference vector is a constant array and given as a common table expression.
`<DistanceFunction>` computes the distance between the reference point and all stored points.
Any of the available [distance function](/sql-reference/functions/distance-functions) can be used for that.
`N` specifies how many neighbors should be returned.

## Exact Nearest Neighbor Search {#exact-nearest-neighbor-search}

An exact nearest neighbor search can be performed using above SELECT query as is.
The runtime of such queries is generally proportional to the number of stored vectors and their dimension, i.e. the number of array elements.
Also, since ClickHouse performs a brute-force scan of all vectors, the runtime depends also on the number of threads by the query (see setting [max_threads](../../../operations/settings/settings.md#max_threads)).

One common approach to speed up exact nearest neighbor search is to use a lower-precision [float data type](../../../sql-reference/data-types/float.md).
For example, if the vectors are stored as `Array(BFloat16)` instead of `Array(Float32)`, then the data size is cut in half, and the query runtimes are expected to go down by half as well.
This method is know as quantization and it might reduce the result accuracy despite the exhaustive scan of all vectors.
If the precision loss is acceptable depends on the use case and typically requires experimentation.

## Example {#exact-nearest-neighbor-search-example}

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

returns

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

# Approximate Nearest Neighbor Search {#approximate-nearest-neighbor-search}

ClickHouse provides a special "vector similarity" index to perform approximate nearest neighbor search.

:::note
Vector similarity indexes are currently experimental.
To enable them, please first run `SET allow_experimental_vector_similarity_index = 1`.
If you run into problems, kindly open an issue at github.com/clickhouse/clickhouse/issues.
:::

## Creating a Vector Similarity Index {#creating-a-vector-similarity-index}

A vector similarity index can be created on a new table like this:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

Alternatively, to add a vector similarity index to an existing table:

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY N];
```

Vector similarity indexes are special kinds of skipping indexes (see [here](mergetree.md#table_engine-mergetree-data_skipping-indexes) and [here](../../../optimize/skipping-indexes)).
Accordingly, above `ALTER TABLE` statement only causes the index to be build for future new data inserted into the table.
To build the index for existing data as well, you need to materialize it:

```sql
ALTER TABLE table MATERIALIZE <index_name> SETTINGS mutations_sync = 2;
```

Function `<distance_function>` must be either
- `L2Distance`, the [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance), representing the length of a line between two points in Euclidean space, or
- `cosineDistance`, the [cosine distance](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance), representing the angle between two non-zero vectors.

For normalized data, `L2Distance` is usually the best choice, otherwise `cosineDistance` is recommended to compensate for scale.

`<dimensions>` restricts the number of elements which every array in the underlying column must have (the value must be > 0).
If ClickHouse finds an array with a different number of elements during index creation, the index is discarded and an error is returned.

The GRANULARITY parameter `<N>` refers to the size of the index granules (see [here](../../../optimize/skipping-indexes)).
The default value of 100 million should work reasonably well for most use cases but it can also be tuned.
We recommend tuning only for advanced users who understand the implications of what they are doing (see below).

Vector similarity indexes are generic in the sense that they can accommodate different approximate search method.
The actually used method is specified by parameter `<type>`.
As of now, the only available method is HNSW ([academic paper](https://arxiv.org/abs/1603.09320)), a popular and state-of-the-art technique for approximate vector search based on hierarchical proximity graphs.
If HNSW is used as type, users may optionally specify further HNSW-specific parameters:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, quantization, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

These HNSW-specific parameters are available:
- `quantization` controls the quantization of the vectors in the proximity graph. Possible values are `f64`, `f32`, `f16`, `bf16`, or `i8`. The default value is `bf16`. Note that this parameter does not affect the representation of the vectors in the underlying column.
- `hnsw_max_connections_per_layer` controls the number of neighbors per graph node, also known as HNSW parameter `M`. The default value is `32`. Value `0` means using the default value.
- `hnsw_candidate_list_size_for_construction` controls the size of the dynamic candidate list during construction of the HNSW graph, also known as HNSW parameter `ef_construction`. The default value is `128`. Value `0` means using the default value.

All HNSW-specific parameters have reasonable default values which work well in the majority of use cases.
We therefore do not recommend to customize HNSW-specific parameters.

Further restrictions apply:
- Vector similarity indexes can only be build on columns of type [Array(Float32)](../../../sql-reference/data-types/array.md) or [Array(Float64)](../../../sql-reference/data-types/array.md). Arrays of nullable and low-cardinality floats such as `Array(Nullable(Float32))` and `Array(LowCardinality(Float32))` are not allowed.
- Vector similarity indexes must be build on single columns.
- Vector similarity indexes may be build on calculated expressions (e.g., `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`) but such indexes cannot be used for approximate neighbor search later on.
- Vector similarity indexes require that all arrays in the underlying column have `<dimension>`-many elements. This is checked during index creation. To detect violations of this requirement as soon as possible, users can add a [constraint](/sql-reference/statements/create/table.md#constraints) for the vector column, e.g., `CONSTRAINT same_length CHECK length(vectors) = 256`.
- Likewise, array values in the underlying column must not be (`[]`) or have a default value (also `[]`).

## Using a Vector Similarity Index {#using-a-vector-similarity-index}

:::note
To use vector similarity indexes, setting [compatibility](../../../operations/settings/settings.md) has be `''` (the default value), or `'25.1'` or newer.
:::

Vector similarity indexes support SELECT queries of this form:

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

ClickHouse's query optimizer tries to match above query template and make use of available vector similarity indexes.
A query can only use a vector similarity index if the distance function in the SELECT query is the same as the distance function in the index definition.

Advanced users may provide a custom value for setting `hnsw_candidate_list_size_for_search` (also know as HNSW parameter `ef_search`) to tune the size of the candidate list during search (e.g.  `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`).
The default value of the setting 256 works well in the majority of use cases.
Higher setting values mean better accuracy at the cost of slower performance.

If the query can use a vector similarity index, ClickHouse checks that the LIMIT `<N>` provided in SELECT queries is within reasonable bounds.
More specifically, an error is returned if `<N>` is bigger than the value of setting `max_limit_for_ann_queries` with default value 100.
Too large LIMITs can slow down searches and usually indicate a usage error.

To check if a SELECT query uses a vector similarity index, you can prefix the query with `EXPLAIN indexes = 1`.

As an example, query

```sql
EXPLAIN indexes = 1
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

may return

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 4/4                                                                       │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 2/4                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Vector similarity indexes are used if the output contains `Skip` and the name and type of the vector index (in the example, `idx` and `vector_similarity`).
In this case, the vector similarity index dropped two of four granules, i.e. 50% of the data.
The more granules can be dropped, the more effective index usage becomes.

:::tip
To enforce index usage, you can run the SELECT query with setting [force_data_skipping_indexes](../../../operations/settings/settings#force_data_skipping_indices) (provide the index name as setting value).
:::

** Post-filtering and Pre-filtering**

Users may optionally specify a `WHERE` clause with additional filter conditions in SELECT queries.
Depending on these filter conditions, ClickHouse will utilize post-filtering or pre-filtering.
These two strategies determine the order in which the filters are evaluated:
- With post-filtering, the vector similarity index is evaluated first, afterwards ClickHouse evaluates the additional filter(s) specified of the `WHERE` clause.
- With pre-filtering, the filter evaluation order is the other way round.

Both strategies have different trade-offs:
- Post-filtering has the general problem that it may return less than the number of rows requested in the `LIMIT <N>` clause. This happens when at least one of the result rows returned by the vector similarity index fails to satisfy the additional filters. In ClickHouse, this situation is luckily unlikely to happen because vector similarity indexes do not return rows but blocks with thousands of rows (see "Differences to Regular Skipping Indexes" below).
- Pre-filtering is an unsolved problem. Some specialized vector databases implement it but most databases including ClickHouse will fall back to exact neighbor search, i.e., a brute-force scan without index.

What strategy is used comes down to whether ClickHouse can use indexes for the additional filter conditions.

If no index can be used, post-filtering will be applied.

If the additional filter condition is part of the partition key, then ClickHouse will apply partition pruning.

Example, assuming that the table is range-partitioned by `year`:

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse will ignore all partitions but the one for year 2025.
Within this partition, a post-filtering strategy will be applied.

If the additional filter condition is part of the primary key, then ClickHouse will always apply pre-filtering.

## Administration {#administration}

The on-disk size of vector similarity indexes can be obtained from [system.data_skipping_indices](../../../operations/system-tables/data_skipping_indices):

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

Example output:

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

## Performance Tuning {#performance-tuning}

**Tuning Index Creation**

The life cycle of vector similarity indexes is tied to the life cycle of parts.
In other words, whenever a new part with defined vector similarity index is created, the index is create as well.
This typically happens when data is [inserted](https://clickhouse.com/docs/guides/inserting-data) or during [merges](https://clickhouse.com/docs/merges).
Unfortunately, HNSW is known for long index creation times which can significantly slow down inserts and merges.
Vector similarity indexes are ideally only used if the data is immutable or rarely changed.

To speed up index creation, the following techniques can be used:

First, index creation can be parallelized.
The maximum number of index creation threads can be configured using server setting [max_build_vector_similarity_index_thread_pool_size](/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size).
For optimal performance, the setting value should be configured to the number of CPU cores.

Second, to speed up INSERT statements, users may disable the creation of skipping indexes on newly inserted parts using session setting [materialize_skip_indexes_on_insert](../../../operations/settings/settings.md).
SELECT queries on such parts will fall back to exact search.
Since inserted parts tend to be small compared to the total table size, the performance impact of that is expected to be negligible.

Third, to speed up merges, users may disable the creation of skipping indexes on merged parts using session setting [materialize_skip_indexes_on_merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge).
This, in conjunction with statement [ALTER TABLE \[...\] MATERIALIZE INDEX \[...\]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index), provides explicit control over the life cycle of vector similarity indexes.
For example, index creation can be deferred until all data was ingested or until a period of low system load such as the weekend.

**Tuning Index Usage**

SELECT queries need to load vector similarity indexes into main memory to use them.
To avoid that the same vector similarity index is loaded repeatedly into main memory, ClickHouse provides a dedicated in-memory cache for such indexes.
The bigger this cache is, the fewer unnecessary loads will happen.
The maximum cache size can be configured using server setting [vector_similarity_index_cache_size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size).
By default, the cache can grow up to 5 GB in size.

The current cache size is shown in [system.metrics](../../../operations/system-tables/metrics.md):

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheSize'
```

The cache hits and misses for a query with some query id can be obtained from [system.query_log](../../../operations/system-tables/query_log.md):

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

For production use-cases, we recommend that the cache is sized large enough so that all vector indexes remain in memory at all times.

## Differences to Regular Skipping Indexes {#differences-to-regular-skipping-indexes}

As all regular [skipping indexes](/optimize/skipping-indexes), vector similarity indexes are constructed over granules and each indexed block consists of `GRANULARITY = [N]`-many granules (`[N]` = 1 by default for normal skipping indexes).
For example, if the primary index granularity of the table is 8192 (setting `index_granularity = 8192`) and `GRANULARITY = 2`, then each indexed block will contain 16384 rows.
However, data structures and algorithms for approximate neighbor search are inherently row-oriented.
They store a compact representation of a set of rows and also return rows for vector search queries.
This causes some rather unintuitive differences in the way vector similarity indexes behave compared to normal skipping indexes.

When a user defines a vector similarity index on a column, ClickHouse internally creates a vector similarity "sub-index" for each index block.
The sub-index is "local" in the sense that it only knows about the rows of its containing index block.
In the previous example and assuming that a column has 65536 rows, we obtain four index blocks (spanning eight granules) and a vector similarity sub-index for each index block.
A sub-index is theoretically able to return the rows with the N closest points within its index block directly.
However, since ClickHouse loads data from disk to memory at the granularity of granules, sub-indexes extrapolate matching rows to granule granularity.
This is different from regular skipping indexes which skip data at the granularity of index blocks.

The `GRANULARITY` parameter determines how many vector similarity sub-indexes are created.
Bigger `GRANULARITY` values mean fewer but larger vector similarity sub-indexes, up to the point where a column (or a column's data part) has only a single sub-index.
In that case, the sub-index has a "global" view of all column rows and can directly return all granules of the column (part) with relevant rows (there are at most `LIMIT [N]`-many such granules).
In a second step, ClickHouse will load these granules and identify the actually best rows by performing a brute-force distance calculation over all rows of the granules.
With a small `GRANULARITY` value, each of the sub-indexes returns up to `LIMIT N`-many granules.
As a result, more granules need to be loaded and post-filtered.
Note that the search accuracy is with both cases equally good, only the processing performance differs.
It is generally recommended to use a large `GRANULARITY` for vector similarity indexes and fall back to a smaller `GRANULARITY` values only in case of problems like excessive memory consumption of the vector similarity structures.
If no `GRANULARITY` was specified for vector similarity indexes, the default value is 100 million.

## Example {#approximate-nearest-neighbor-search-example}

```sql
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

returns

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

# References {#references}

Blogs:
- [Vector Search with ClickHouse - Part 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
- [Vector Search with ClickHouse - Part 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)
