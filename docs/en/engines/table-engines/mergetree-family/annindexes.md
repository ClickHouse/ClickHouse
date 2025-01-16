---
slug: /en/engines/table-engines/mergetree-family/annindexes
sidebar_label: Vector Similarity Indexes
description: Approximate Nearest Neighbor Search with Vector Similarity Indexes
keywords: [vector-similarity search, text search, ann, indices, index, nearest neighbour]
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import PrivatePreviewBadge from '@theme/badges/PrivatePreviewBadge';

# Approximate Nearest Neighbor Search with Vector Similarity Indexes

<ExperimentalBadge/>
<PrivatePreviewBadge/>

Nearest neighborhood search is the problem of finding the M closest vectors to a given vector in an N-dimensional vector space. The most
straightforward approach to solve this problem is an exhaustive (brute-force) search which computes the distance between the reference
vector and all other points in the vector space. While method guarantees a perfectly accurate result, but it is usually too slow for
practical applications. As an alternative, [approximative algorithms](https://github.com/erikbern/ann-benchmarks) use greedy heuristics to
find the M closest vectors much faster. This allows to semantic search of picture, song, text
[embeddings](https://cloud.google.com/architecture/overview-extracting-and-serving-feature-embeddings-for-machine-learning) in milliseconds.

Blogs:
- [Vector Search with ClickHouse - Part 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
- [Vector Search with ClickHouse - Part 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)


In terms of SQL, a nearest neighborhood search can be expressed as follows:

``` sql
SELECT [...]
FROM table, [...]
ORDER BY DistanceFunction(vectors, reference_vector)
LIMIT N
```

where
- `DistanceFunction` computes a distance between two vectors (e.g. the
  [L2Distance](../../../sql-reference/functions/distance-functions.md#L2Distance) or
  [cosineDistance](../../../sql-reference/functions/distance-functions.md#cosineDistance)),
- `vectors` is a column of type [Array(Float64)](../../../sql-reference/data-types/array.md) or
  [Array(Float32)](../../../sql-reference/data-types/array.md), or [Array(BFloat16)](../../../sql-reference/data-types/array.md), typically
  storing embeddings,
- `reference_vector` is a literal of type [Array(Float64)](../../../sql-reference/data-types/array.md) or
  [Array(Float32)](../../../sql-reference/data-types/array.md), or [Array(BFloat16)](../../../sql-reference/data-types/array.md), and
- `N` is a constant integer restricting the number of returned results.

The query returns the `N` closest points in `vectors` to `reference_vector`.

Exhaustive search computes the distance between `reference_vector` and all vectors in `vectors`. As such, its runtime is linear in the
number of stored vectors. Approximate search relies on special data structures (e.g. graphs, random forests, etc.) which allow to find the
closest vectors to a given reference vector quickly (i.e. in sub-linear time). ClickHouse provides such a data structure in the form of
"vector similarity indexes", a type of [skipping index](mergetree.md#table_engine-mergetree-data_skipping-indexes).

# Creating and Using Vector Similarity Indexes

Syntax to create a vector similarity index

```sql
CREATE TABLE table
(
  id Int64,
  vectors Array(Float32),
  INDEX index_name vectors TYPE vector_similarity(method, distance_function[, quantization, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY id;
```

:::note
USearch indexes are currently experimental, to use them you first need to `SET allow_experimental_vector_similarity_index = 1`.
:::

The index can be build on a column of type [Array(Float64)](../../../sql-reference/data-types/array.md),
[Array(Float32)](../../../sql-reference/data-types/array.md), or [Array(BFloat16)](../../../sql-reference/data-types/array.md).

Index parameters:
- `method`: Currently only `hnsw` is supported.
- `distance_function`: either `L2Distance` (the [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance): the length of a line
  between two points in Euclidean space), or `cosineDistance` (the [cosine
  distance](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance): the angle between two non-zero vectors).
- `quantization`: either `f64`, `f32`, `f16`, `bf16`, or `i8` for storing vectors with reduced precision (optional, default: `bf16`)
- `hnsw_max_connections_per_layer`: the number of neighbors per HNSW graph node, also known as `M` in the [HNSW
  paper](https://doi.org/10.1109/TPAMI.2018.2889473). Optional, default: `32`. Value `0` means using the default value.
- `hnsw_candidate_list_size_for_construction`: the size of the dynamic candidate list when constructing the HNSW graph, also known as
  `ef_construction` in the original [HNSW paper](https://doi.org/10.1109/TPAMI.2018.2889473). Optional, default: `128`. Value 0 means using
  the default value.

For normalized data, `L2Distance` is usually the best choice, otherwise `cosineDistance` is recommended to compensate for scale.

Example:

```sql
CREATE TABLE table
(
  id Int64,
  vectors Array(Float32),
  INDEX idx vectors TYPE vector_similarity('hnsw', 'L2Distance') -- Alternative syntax: TYPE vector_similarity(hnsw, L2Distance)
)
ENGINE = MergeTree
ORDER BY id;
```

All arrays must have same length. To avoid errors, you can use a
[CONSTRAINT](/docs/en/sql-reference/statements/create/table.md#constraints), for example, `CONSTRAINT constraint_name_1 CHECK
length(vectors) = 256`. Empty `Arrays` and unspecified `Array` values in INSERT statements (i.e. default values) are not supported as well.

Vector similarity indexes are based on the [USearch library](https://github.com/unum-cloud/usearch), which implements the [HNSW
algorithm](https://arxiv.org/abs/1603.09320), i.e., a hierarchical graph where each node represents a vector and the edges between nodes
represent similarity. Such hierarchical structures can be very efficient on large collections. They may often fetch 0.05% or less data from
the overall dataset, while still providing 99% recall. This is especially useful when working with high-dimensional vectors which are
expensive to load and compare. USearch also utilizes SIMD to accelerate distance computations on modern x86 (AVX2 and AVX-512) and ARM (NEON
and SVE) CPUs.

Vector similarity indexes are built during column insertion and merge. The HNSW algorithm is known to provide slow inserts. As a result,
`INSERT` and `OPTIMIZE` statements on tables with vector similarity index will be slower than for ordinary tables. Vector similarity indexes
are ideally used only with immutable or rarely changed data, respectively when are far more read requests than write requests. Three
additional techniques are recommended to speed up index creation:
- Index creation can be parallelized. The maximum number of threads can be configured using server setting
  [max_build_vector_similarity_index_thread_pool_size](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters_max_build_vector_similarity_index_thread_pool_size).
- Index creation on newly inserted parts may be disabled using setting `materialize_skip_indexes_on_insert`. Search on such parts will fall
  back to exact search but since inserted parts are typically small compared to the total table size, the performance impact is negligible.
- ClickHouse merges multiple parts incrementally in the background into bigger parts. These new parts are potentially merged later into even
  bigger parts. Each merge re-builds the vector similarity index the output part (as well as other skip indexes) every time from
  scratch. This potentially wastes work for creating vector similarity indexes. To avoid that, it is possible to suppress the creation of
  vector similarity indexes during merge using merge tree setting
  [materialize_skip_indexes_on_merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge). This, in
  conjunction with statement [ALTER TABLE \[...\] MATERIALIZE INDEX
  \[...\]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index), provides explicit control over the life cycle of
  vector similarity indexes. For example, index building can be deferred to periods of low load (e.g. weekends) or after a large data
  ingestion.

Vector similarity indexes support this type of query:

``` sql
WITH [...] AS reference_vector
SELECT *
FROM table
WHERE ...                       -- WHERE clause is optional
ORDER BY Distance(vectors, reference_vector)
LIMIT N
```

To search using a different value of HNSW parameter `hnsw_candidate_list_size_for_search` (default: 256), also known as `ef_search` in the
original [HNSW paper](https://doi.org/10.1109/TPAMI.2018.2889473), run the `SELECT` query with `SETTINGS hnsw_candidate_list_size_for_search
= <value>`.

Repeated reads from vector similarity indexes benefit from a large skipping index cache. If needed, you can increase the default cache size
using server setting [skipping_index_cache_size](../../../operations/server-configuration-parameters/settings.md#skipping_index_cache_size).

**Restrictions**: Approximate vector search algorithms require a limit, hence queries without `LIMIT` clause cannot utilize vector
similarity indexes. The limit must also be smaller than setting `max_limit_for_ann_queries` (default: 100).

**Differences to Regular Skip Indexes** Similar to regular [skip indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes), vector
similarity indexes are constructed over granules and each indexed block consists of `GRANULARITY = [N]`-many granules (`[N]` = 1 by default
for normal skip indexes). For example, if the primary index granularity of the table is 8192 (setting `index_granularity = 8192`) and
`GRANULARITY = 2`, then each indexed block will contain 16384 rows. However, data structures and algorithms for approximate neighborhood
search are inherently row-oriented. They store a compact representation of a set of rows and also return rows for vector search queries.
This causes some rather unintuitive differences in the way vector vector similarity indexes behave compared to normal skip indexes.

When a user defines an vector similarity index on a column, ClickHouse internally creates an vector similarity "sub-index" for each index
block. The sub-index is "local" in the sense that it only knows about the rows of its containing index block. In the previous example and
assuming that a column has 65536 rows, we obtain four index blocks (spanning eight granules) and an vector similarity sub-index for each
index block. A sub-index is theoretically able to return the rows with the N closest points within its index block directly. However, since
ClickHouse loads data from disk to memory at the granularity of granules, sub-indexes extrapolate matching rows to granule granularity. This
is different from regular skip indexes which skip data at the granularity of index blocks.

The `GRANULARITY` parameter determines how many vector similarity sub-indexes are created. Bigger `GRANULARITY` values mean fewer but larger
vector similarity sub-indexes, up to the point where a column (or a column's data part) has only a single sub-index. In that case, the
sub-index has a "global" view of all column rows and can directly return all granules of the column (part) with relevant rows (there are at
most `LIMIT [N]`-many such granules). In a second step, ClickHouse will load these granules and identify the actually best rows by
performing a brute-force distance calculation over all rows of the granules. With a small `GRANULARITY` value, each of the sub-indexes
returns up to `LIMIT N`-many granules. As a result, more granules need to be loaded and post-filtered. Note that the search accuracy is with
both cases equally good, only the processing performance differs. It is generally recommended to use a large `GRANULARITY` for vector
similarity indexes and fall back to a smaller `GRANULARITY` values only in case of problems like excessive memory consumption of the vector
similarity structures. If no `GRANULARITY` was specified for vector similarity indexes, the default value is 100 million.
