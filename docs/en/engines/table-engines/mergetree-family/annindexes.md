---
description: 'Documentation for Exact and Approximate Vector Search'
keywords: ['vector similarity search', 'ann', 'knn', 'hnsw', 'indices', 'index', 'nearest neighbor', 'vector search']
sidebar_label: 'Exact and Approximate Vector Search'
slug: /engines/table-engines/mergetree-family/annindexes
title: 'Exact and Approximate Vector Search'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# Exact and approximate vector search

The problem of finding the N closest points in a multi-dimensional (vector) space for a given point is known as [nearest neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) or, in short: vector search.
Two general approaches exist for solving vector search:
- Exact vector search calculates the distance between the given point and all points in the vector space. This ensures the best possible accuracy, i.e. the returned points are guaranteed to be the actual nearest neighbors. Since the vector space is explored exhaustively, exact vector search can be too slow for real-world use.
- Approximate vector search refers to a group of techniques (e.g., special data structures like graphs and random forests) which compute results much faster than exact vector search. The result accuracy is typically "good enough" for practical use. Many approximate techniques provide parameters to tune the trade-off between the result accuracy and the search time.

A vector search (exact or approximate) can be written in SQL as follows:

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
`<N>` specifies how many neighbors should be returned.

## Exact vector search {#exact-nearest-neighbor-search}

An exact vector search can be performed using above SELECT query as is.
The runtime of such queries is generally proportional to the number of stored vectors and their dimension, i.e. the number of array elements.
Also, since ClickHouse performs a brute-force scan of all vectors, the runtime depends also on the number of threads by the query (see setting [max_threads](../../../operations/settings/settings.md#max_threads)).

### Example {#exact-nearest-neighbor-search-example}

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

## Approximate vector search {#approximate-nearest-neighbor-search}

### Vector Similarity Indexes {#vector-similarity-index}

ClickHouse provides a special "vector similarity" index to perform approximate vector search.

:::note
Vector similarity indexes are available in ClickHouse version 25.8 and higher.
If you run into problems, kindly open an issue in the [ClickHouse repository](https://github.com/clickhouse/clickhouse/issues).
:::

#### Creating a Vector Similarity Index {#creating-a-vector-similarity-index}

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
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

Vector similarity indexes are special kinds of skipping indexes (see [here](mergetree.md#table_engine-mergetree-data_skipping-indexes) and [here](../../../optimize/skipping-indexes)).
Accordingly, above `ALTER TABLE` statement only causes the index to be build for future new data inserted into the table.
To build the index for existing data as well, you need to materialize it:

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

Function `<distance_function>` must be
- `L2Distance`, the [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance), representing the length of a line between two points in Euclidean space, or
- `cosineDistance`, the [cosine distance](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance), representing the angle between two non-zero vectors.

For normalized data, `L2Distance` is usually the best choice, otherwise `cosineDistance` is recommended to compensate for scale.

`<dimensions>` specifies the array cardinality (number of elements) in the underlying column.
If ClickHouse finds an array with a different cardinality during index creation, the index is discarded and an error is returned.

The optional GRANULARITY parameter `<N>` refers to the size of the index granules (see [here](../../../optimize/skipping-indexes)).
The default value of 100 million should work reasonably well for most use cases but it can also be tuned.
We recommend tuning only for advanced users who understand the implications of what they are doing (see [below](#differences-to-regular-skipping-indexes)).

Vector similarity indexes are generic in the sense that they can accommodate different approximate search method.
The actually used method is specified by parameter `<type>`.
As of now, the only available method is HNSW ([academic paper](https://arxiv.org/abs/1603.09320)), a popular and state-of-the-art technique for approximate vector search based on hierarchical proximity graphs.
If HNSW is used as type, users may optionally specify further HNSW-specific parameters:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

These HNSW-specific parameters are available:
- `<quantization>` controls the quantization of the vectors in the proximity graph. Possible values are `f64`, `f32`, `f16`, `bf16`, `i8`, or `b1`. The default value is `bf16`. Note that this parameter does not affect the representation of the vectors in the underlying column.
- `<hnsw_max_connections_per_layer>` controls the number of neighbors per graph node, also known as HNSW hyperparameter `M`. The default value is `32`. Value `0` means using the default value.
- `<hnsw_candidate_list_size_for_construction>` controls the size of the dynamic candidate list during construction of the HNSW graph, also known as HNSW hyperparameter `ef_construction`. The default value is `128`. Value `0` means using the default value.

The default values of all HNSW-specific parameters work reasonably well in the majority of use cases.
We therefore do not recommend customizing the HNSW-specific parameters.

Further restrictions apply:
- Vector similarity indexes can only be build on columns of type [Array(Float32)](../../../sql-reference/data-types/array.md), [Array(Float64)](../../../sql-reference/data-types/array.md), or [Array(BFloat16)](../../../sql-reference/data-types/array.md). Arrays of nullable and low-cardinality floats such as `Array(Nullable(Float32))` and `Array(LowCardinality(Float32))` are not allowed.
- Vector similarity indexes must be build on single columns.
- Vector similarity indexes may be build on calculated expressions (e.g., `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`) but such indexes cannot be used for approximate neighbor search later on.
- Vector similarity indexes require that all arrays in the underlying column have `<dimension>`-many elements - this is checked during index creation. To detect violations of this requirement as early as possible, users can add a [constraint](/sql-reference/statements/create/table.md#constraints) for the vector column, e.g., `CONSTRAINT same_length CHECK length(vectors) = 256`.
- Likewise, array values in the underlying column must not be empty (`[]`) or have a default value (also `[]`).

**Estimating storage and memory consumption**

A vector generated for use with a typical AI model (e.g. a Large Language Model, [LLMs](https://en.wikipedia.org/wiki/Large_language_model)) consists of hundreds or thousands of floating-point values.
Thus, a single vector value can have a memory consumption of multiple kilobyte.
Users who like to estimate the storage required for the underlying vector column in the table, as well as the main memory needed for the vector similarity index, can use below two formula:

Storage consumption of the vector column in the table (uncompressed):

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

Example for the [dbpedia dataset](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M):

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

The vector similarity index must be fully loaded from disk into main memory to perform searches.
Similarly, the vector index is also constructed fully in memory and then saved to disk.

Memory consumption required to load a vector index:

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

Example for the [dbpedia dataset](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M):

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

Above formula does not account for additional memory required by vector similarity indexes to allocate runtime data structures like pre-allocated buffers and caches.

#### Using a Vector Similarity Index {#using-a-vector-similarity-index}

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

Advanced users may provide a custom value for setting [hnsw_candidate_list_size_for_search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search) (also know as HNSW hyperparameter "ef_search") to tune the size of the candidate list during search (e.g.  `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`).
The default value of the setting 256 works well in the majority of use cases.
Higher setting values mean better accuracy at the cost of slower performance.

If the query can use a vector similarity index, ClickHouse checks that the LIMIT `<N>` provided in SELECT queries is within reasonable bounds.
More specifically, an error is returned if `<N>` is bigger than the value of setting [max_limit_for_vector_search_queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries) with default value 100.
Too large LIMIT values can slow down searches and usually indicate a usage error.

To check if a SELECT query uses a vector similarity index, you can prefix the query with `EXPLAIN indexes = 1`.

As an example, query

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
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
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

In this example, 1 million vectors in the [dbpedia dataset](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M), each with dimension 1536, are stored in 575 granules, i.e. 1.7k rows per granule.
The query asks for 10 neighbours and the vector similarity index finds these 10 neighbours in 10 separate granules.
These 10 granules will be read during query execution.

Vector similarity indexes are used if the output contains `Skip` and the name and type of the vector index (in the example, `idx` and `vector_similarity`).
In this case, the vector similarity index dropped two of four granules, i.e. 50% of the data.
The more granules can be dropped, the more effective index usage becomes.

:::tip
To enforce index usage, you can run the SELECT query with setting [force_data_skipping_indexes](../../../operations/settings/settings#force_data_skipping_indices) (provide the index name as setting value).
:::

**Post-filtering and Pre-filtering**

Users may optionally specify a `WHERE` clause with additional filter conditions for the SELECT query.
ClickHouse will evaluate these filter conditions using post-filtering or pre-filtering strategy.
In short, both strategies determine the order in which the filters are evaluated:
- Post-filtering means that the vector similarity index is evaluated first, afterwards ClickHouse evaluates the additional filter(s) specified in the `WHERE` clause.
- Pre-filtering means that the filter evaluation order is the other way round.

The strategies have different trade-offs:
- Post-filtering has the general problem that it may return less than the number of rows requested in the `LIMIT <N>` clause. This situation happens when one or more result rows returned by the vector similarity index fails to satisfy the additional filters.
- Pre-filtering is generally an unsolved problem. Certain specialized vector databases provide pre-filtering algorithms but most relational databases (including ClickHouse) will fall back to exact neighbor search, i.e., a brute-force scan without index.

What strategy is used depends on the filter condition.

*Additional filters are part of the partition key*

If the additional filter condition is part of the partition key, then ClickHouse will apply partition pruning.
As an example, a table is range-partitioned by column `year` and the following query is run:

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse will prune all partitions except the 2025 one.

*Additional filters cannot be evaluated using indexes*

If additional filter conditions cannot be evaluated using indexes (primary key index, skipping index), ClickHouse will apply post-filtering.

*Additional filters can be evaluated using the primary key index*

If additional filter conditions can be evaluated using the [primary key](mergetree.md#primary-key) (i.e., they form a prefix of the primary key) and
- the filter condition eliminates at least one row within a part, the ClickHouse will fall back to pre-filtering for the "surviving" ranges within the part,
- the filter condition eliminates no rows within a part, the ClickHouse will perform post-filtering for the part.

In practical use cases, the latter case is rather unlikely.

*Additional filters can be evaluated using skipping index*

If additional filter conditions can be evaluated using [skipping indexes](mergetree.md#table_engine-mergetree-data_skipping-indexes) (minmax index, set index, etc.), Clickhouse performs post-filtering.
In such cases, the vector similarity index is evaluated first as it is expected to remove the most rows relative to other skipping indexes.

For finer control over post-filtering vs. pre-filtering, two settings can be used:

Setting [vector_search_filter_strategy](../../../operations/settings/settings#vector_search_filter_strategy) (default: `auto` which implements above heuristics) may be set to `prefilter`.
This is useful to force pre-filtering in cases where the additional filter conditions are extremely selective.
As an example, the following query may benefit from pre-filtering:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

Assuming that only a very small number of books cost less than 2 dollar, post-filtering may return zero rows because the top 10 matches returned by the vector index could all be priced above 2 dollar.
By forcing pre-filtering (add `SETTINGS vector_search_filter_strategy = 'prefilter'` to the query), ClickHouse first finds all books with a price of less than 2 dollar and then executes a brute-force vector search for the found books.

As an alternative approach to resolve above issue, setting [vector_search_index_fetch_multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier) (default: `1.0`, maximum: `1000.0`) may be configured to a value > `1.0` (for example, `2.0`).
The number of nearest neighbors fetched from the vector index is multiplied by the setting value and then the additional filter to be applied on those rows to return LIMIT-many rows.
As an example, we can query again but with multiplier `3.0`:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

ClickHouse will fetch 3.0 x 10 = 30 nearest neighbors from the vector index in each part and afterwards evaluate the additional filters.
Only the ten closest neighbors will be returned.
We note that setting `vector_search_index_fetch_multiplier` can mitigate the problem but in extreme cases (very selective WHERE condition), it is still possible that less than N requested rows returned.

**Rescoring**

Skip indexes in ClickHouse generally filter at the granule level, i.e. a lookup in a skip index (internally) returns a list of potentially matching granules which reduces the number of read data in the subsequent scan.
This works well for skip indexes in general but in the case of vector similarity indexes, it creates a "granularity mismatch".
In more detail, the vector similarity index determines the row numbers of the N most similar vectors for a given reference vector, but it then needs to extrapolate these row numbers to granule numbers.
ClickHouse will then load these granules from disk, and repeat the distance calculation for all vectors in these granules.
This step is called rescoring and while it can theoretically improve accuracy - remember the vector similarity index returns only an _approximate_ result, it is obvious not optimal in terms of performance.

ClickHouse therefore provides an optimization which disables rescoring and returns the most similar vectors and their distances directly from the index.
The optimization is enabled by default, see setting [vector_search_with_rescoring](../../../operations/settings/settings#vector_search_with_rescoring).
The way it works at a high level is that ClickHouse makes the most similar vectors and their distances available as a virtual column `_distances`.
To see this, run a vector search query with `EXPLAIN header = 1`:

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
A query run without rescoring (`vector_search_with_rescoring = 0`) and with parallel replicas enabled may fall back to rescoring.
:::

#### Performance tuning {#performance-tuning}

**Tuning compression**

In virtually all use cases, the vectors in the underlying column are dense and do not compress well.
As a result, [compression](/sql-reference/statements/create/table.md#column_compression_codec) slows down inserts and reads into/from the vector column.
We therefore recommend to disable compression.
To do that, specify `CODEC(NONE)` for the vector column like this:

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**Tuning index creation**

The life cycle of vector similarity indexes is tied to the life cycle of parts.
In other words, whenever a new part with defined vector similarity index is created, the index is create as well.
This typically happens when data is [inserted](https://clickhouse.com/docs/guides/inserting-data) or during [merges](https://clickhouse.com/docs/merges).
Unfortunately, HNSW is known for long index creation times which can significantly slow down inserts and merges.
Vector similarity indexes are ideally only used if the data is immutable or rarely changed.

To speed up index creation, the following techniques can be used:

First, index creation can be parallelized.
The maximum number of index creation threads can be configured using server setting [max_build_vector_similarity_index_thread_pool_size](/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size).
For optimal performance, the setting value should be configured to the number of CPU cores.

Second, to speed up INSERT statements, users may disable the creation of skipping indexes on newly inserted parts using session setting [materialize_skip_indexes_on_insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert).
SELECT queries on such parts will fall back to exact search.
Since inserted parts tend to be small compared to the total table size, the performance impact of that is expected to be negligible.

Third, to speed up merges, users may disable the creation of skipping indexes on merged parts using session setting [materialize_skip_indexes_on_merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge).
This, in conjunction with statement [ALTER TABLE \[...\] MATERIALIZE INDEX \[...\]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index), provides explicit control over the life cycle of vector similarity indexes.
For example, index creation can be deferred until all data was ingested or until a period of low system load such as the weekend.

**Tuning index usage**

SELECT queries need to load vector similarity indexes into main memory to use them.
To avoid that the same vector similarity index is loaded repeatedly into main memory, ClickHouse provides a dedicated in-memory cache for such indexes.
The bigger this cache is, the fewer unnecessary loads will happen.
The maximum cache size can be configured using server setting [vector_similarity_index_cache_size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size).
By default, the cache can grow up to 5 GB in size.

:::note
The vector similarity index cache stores vector index granules.
If individual vector index granules are bigger than the cache size, they will not be cached.
Therefore, please make sure to calculate the vector index size (based on the formula in "Estimating storage and memory consumption" or [system.data_skipping_indices](../../../operations/system-tables/data_skipping_indices)) and size the cache correspondingly.
:::

The current size of the vector similarity index cache is shown in [system.metrics](../../../operations/system-tables/metrics.md):

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
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

**Tuning quantization**

[Quantization](https://huggingface.co/blog/embedding-quantization) is a technique to reduce the memory footprint of vectors and the computational costs of building and traversing vector indexes.
ClickHouse vector indexes supports the following quantization options:

| Quantization   | Name                         | Storage per dimension |
|----------------|------------------------------|---------------------- |
| f32            | Single precision             | 4 bytes               |
| f16            | Half precision               | 2 bytes               |
| bf16 (default) | Half precision (brain float) | 2 bytes               |
| i8             | Quarter precision            | 1 byte                |
| b1             | Binary                       | 1 bit                 |

Quantization reduces the precision of vector searches compared to searching the original full-precision floating-point values (`f32`).
However, on most datasets, half-precision brain float quantization (`bf16`) results in a negligible precision loss, therefore vector similarity indexes use this quantization technique by default.
Quarter precision (`i8`) and binary (`b1`) quantization causes appreciable precision loss in vector searches.
We recommend both quantizations only if the the size of the vector similarity index is significantly larger than the available DRAM size.
In this case, we also suggest enabling rescoring ([vector_search_index_fetch_multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier), [vector_search_with_rescoring](../../../operations/settings/settings#vector_search_with_rescoring)) to improve accuracy.
Binary quantization is only recommended for 1) normalized embeddings (i.e. vector length = 1, OpenAI models are usually normalized), and 2) if the cosine distance is used as distance function.
Binary quantization internally uses the Hamming distance to construct and search the proximity graph.
The rescoring step uses the original full-precision vectors stored in the table to identify the nearest neighbours via cosine distance.

**Tuning data transfer**

The reference vector in a vector search query is provided by the user and generally retrieved by making a call to a Large Language Model (LLM).
Typical Python code which runs a vector search in ClickHouse might look like this

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

Embedding vectors (`search_v` in above snippet) could have a very large dimension.
For example, OpenAI provides models that generate embeddings vectors with 1536 or even 3072 dimensions.
In above code, the ClickHouse Python driver substitutes the embedding vector by a human readable string and subsequently send the SELECT query entirely as a string.
Assuming the embedding vector consists of 1536 single-precision floating point values, the sent string reaches a length of 20 kB.
This creates a high CPU usage for tokenizing, parsing and performing thousands of string-to-float conversions.
Also, significant space is required in the ClickHouse server log file, causing bloat in `system.query_log` as well.

Note that most LLM models return an embedding vector as a list or NumPy array of native floats.
We therefore recommend Python applications to bind the reference vector parameter in binary form by using the following style:

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, (SELECT reinterpret($search_v_binary$, 'Array(Float32)')))
    LIMIT 10"
    parameters = params)
```

In the example, the reference vector is sent as-is in binary form and reinterpreted as array of floats on the server.
This saves CPU time on the server side, and avoids bloat in the server logs and `system.query_log`.

#### Administration and monitoring {#administration}

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

#### Differences to regular skipping indexes {#differences-to-regular-skipping-indexes}

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

#### Example {#approximate-nearest-neighbor-search-example}

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

Further example datasets that use approximate vector search:
- [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
- [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
- [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
- [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

### Quantized Bit (QBit) {#approximate-nearest-neighbor-search-qbit}

<ExperimentalBadge/>

One common approach to speed up exact vector search is to use a lower-precision [float data type](../../../sql-reference/data-types/float.md).
For example, if vectors are stored as `Array(BFloat16)` instead of `Array(Float32)`, the data size is reduced by half, and query runtimes are expected to decrease proportionally.
This method is known as quantization. While it speeds up computation, it may reduce result accuracy despite performing an exhaustive scan of all vectors.

With traditional quantization, we lose precision both during search and when storing the data. In the example above, we would store `BFloat16` instead of `Float32`, meaning we can never perform a more accurate search later, even if desired. One alternative approach is to store two copies of the data: quantized and full-precision. While this works, it requires redundant storage. Consider a scenario where we have `Float64` as original data and want to run searches with different precision (16-bit, 32-bit, or full 64-bit). We would need to store three separate copies of the data.

ClickHouse offers the Quantized Bit (`QBit`) data type that addresses these limitations by:
1. Storing the original full-precision data.
2. Allowing quantization precision to be specified at query time.

This is achieved by storing data in a bit-grouped format (meaning all i-th bits of all vectors are stored together), enabling reads at only the requested precision level. You get the speed benefits of reduced I/O and computation from quantization while keeping all original data available when needed. When maximum precision is selected, the search becomes exact.

:::note
The `QBit` data type and its associated distance functions are currently experimental. To enable them, run `SET allow_experimental_qbit_type = 1`.
If you encounter problems, please open an issue in the [ClickHouse repository](https://github.com/clickhouse/clickhouse/issues).
:::

To declare a column of `QBit` type, use the following syntax:

```sql
column_name QBit(element_type, dimension)
```

Where:
* `element_type` – the type of each vector element. Supported types are `BFloat16`, `Float32`, and `Float64`
* `dimension` – the number of elements in each vector

#### Creating a `QBit` Table and Adding Data {#qbit-create}

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

#### Vector Search with `QBit` {#qbit-search}

Let's find the nearest neighbors to a vector representing word 'lemon' using L2 distance. The third parameter in the distance function specifies the precision in bits - higher values provide more accuracy but require more computation.

You can find all available distance functions for `QBit` [here](../../../sql-reference/data-types/qbit.md#vector-search-functions).

**Full precision search (64-bit):**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**Reduced precision search:**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

Notice that with 12-bit quantization, we get a good approximation of the distances with faster query execution. The relative ordering remains largely consistent, with 'apple' still being the closest match.

:::note
In the current state, the speed-up is due to reduced I/O as we read less data. If the original data was wide, like `Float64`, choosing a lower precision will still result in distance calculation on data of the same width – just with less precision.
:::

#### Performance Considerations {#qbit-performance}

The performance benefit of `QBit` comes from reduced I/O operations, as less data needs to be read from storage when using lower precision. Moreover, when the `QBit` contains `Float32` data, if the precision parameter is 16 or below, there will be additional benefits from reduced computation. The precision parameter directly controls the trade-off between accuracy and speed:

- **Higher precision** (closer to the original data width): More accurate results, slower queries
- **Lower precision**: Faster queries with approximate results, reduced memory usage

### References {#references}

Blogs:
- [Vector Search with ClickHouse - Part 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
- [Vector Search with ClickHouse - Part 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)
