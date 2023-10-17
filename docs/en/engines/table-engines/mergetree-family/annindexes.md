# Approximate Nearest Neighbor Search Indexes [experimental] {#table_engines-ANNIndex}

Nearest neighborhood search is the problem of finding the M closest points for a given point in an N-dimensional vector space. The most
straightforward approach to solve this problem is a brute force search where the distance between all points in the vector space and the
reference point is computed. This method guarantees perfect accuracy but it is usually too slow for practical applications. Thus, nearest
neighborhood search problems are often solved with [approximative algorithms](https://github.com/erikbern/ann-benchmarks). Approximative
nearest neighborhood search techniques, in conjunction with [embedding
methods](https://cloud.google.com/architecture/overview-extracting-and-serving-feature-embeddings-for-machine-learning) allow to search huge
amounts of media (pictures, songs, articles, etc.) in milliseconds.

Blogs:
- [Vector Search with ClickHouse - Part 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
- [Vector Search with ClickHouse - Part 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)


In terms of SQL, the nearest neighborhood problem can be expressed as follows:

``` sql
SELECT *
FROM table
ORDER BY Distance(vectors, Point)
LIMIT N
```

`vectors` contains N-dimensional values of type [Array](../../../sql-reference/data-types/array.md) or
[Tuple](../../../sql-reference/data-types/tuple.md), for example embeddings. Function `Distance` computes the distance between two vectors.
Often, the the Euclidean (L2) distance is chosen as distance function but [other
distance functions](/docs/en/sql-reference/functions/distance-functions.md) are also possible. `Point` is the reference point, e.g. `(0.17,
0.33, ...)`, and `N` limits the number of search results.

An alternative formulation of the nearest neighborhood search problem looks as follows:

``` sql
SELECT *
FROM table
WHERE Distance(vectors, Point) < MaxDistance
LIMIT N
```

While the first query returns the top-`N` closest points to the reference point, the second query returns all points closer to the reference
point than a maximally allowed radius `MaxDistance`. Parameter `N` limits the number of returned values which is useful for situations where
`MaxDistance` is difficult to determine in advance.

With brute force search, both queries are expensive (linear in the number of points) because the distance between all points in `vectors` and
`Point` must be computed. To speed this process up, Approximate Nearest Neighbor Search Indexes (ANN indexes) store a compact representation
of the search space (using clustering, search trees, etc.) which allows to compute an approximate answer much quicker (in sub-linear time).

# Creating and Using ANN Indexes

Syntax to create an ANN index over an [Array](../../../sql-reference/data-types/array.md) column:

```sql
CREATE TABLE table
(
  `id` Int64,
  `vectors` Array(Float32),
  INDEX [ann_index_name vectors TYPE [ann_index_type]([ann_index_parameters]) [GRANULARITY [N]]
)
ENGINE = MergeTree
ORDER BY id;
```

Syntax to create an ANN index over a [Tuple](../../../sql-reference/data-types/tuple.md) column:

```sql
CREATE TABLE table
(
  `id` Int64,
  `vectors` Tuple(Float32[, Float32[, ...]]),
  INDEX [ann_index_name] vectors TYPE [ann_index_type]([ann_index_parameters]) [GRANULARITY [N]]
)
ENGINE = MergeTree
ORDER BY id;
```

ANN indexes are built during column insertion and merge. As a result, `INSERT` and `OPTIMIZE` statements will be slower than for ordinary
tables. ANNIndexes are ideally used only with immutable or rarely changed data, respectively when are far more read requests than write
requests.

ANN indexes support two types of queries:

- ORDER BY queries:

  ``` sql
  SELECT *
  FROM table
  [WHERE ...]
  ORDER BY Distance(vectors, Point)
  LIMIT N
  ```

- WHERE queries:

   ``` sql
   SELECT *
   FROM table
   WHERE Distance(vectors, Point) < MaxDistance
   LIMIT N
   ```

:::tip
To avoid writing out large vectors, you can use [query
parameters](/docs/en/interfaces/cli.md#queries-with-parameters-cli-queries-with-parameters), e.g.

```bash
clickhouse-client --param_vec='hello' --query="SELECT * FROM table WHERE L2Distance(vectors, {vec: Array(Float32)}) < 1.0"
```
:::

**Restrictions**: Queries that contain both a `WHERE Distance(vectors, Point) < MaxDistance` and an `ORDER BY Distance(vectors, Point)`
clause cannot use ANN indexes. Also, the approximate algorithms used to determine the nearest neighbors require a limit, hence queries
without `LIMIT` clause cannot utilize ANN indexes. Also ANN indexes are only used if the query has a `LIMIT` value smaller than setting
`max_limit_for_ann_queries` (default: 1 million rows). This is a safeguard to prevent large memory allocations by external libraries for
approximate neighbor search.

**Differences to Skip Indexes** Similar to regular [skip indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes), ANN indexes are
constructed over granules and each indexed block consists of `GRANULARITY = [N]`-many granules (`[N]` = 1 by default for normal skip
indexes). For example, if the primary index granularity of the table is 8192 (setting `index_granularity = 8192`) and `GRANULARITY = 2`,
then each indexed block will contain 16384 rows. However, data structures and algorithms for approximate neighborhood search (usually
provided by external libraries) are inherently row-oriented. They store a compact representation of a set of rows and also return rows for
ANN queries. This causes some rather unintuitive differences in the way ANN indexes behave compared to normal skip indexes.

When a user defines a ANN index on a column, ClickHouse internally creates a ANN "sub-index" for each index block. The sub-index is "local"
in the sense that it only knows about the rows of its containing index block. In the previous example and assuming that a column has 65536
rows, we obtain four index blocks (spanning eight granules) and a ANN sub-index for each index block. A sub-index is theoretically able to
return the rows with the N closest points within its index block directly. However, since ClickHouse loads data from disk to memory at the
granularity of granules, sub-indexes extrapolate matching rows to granule granularity. This is different from regular skip indexes which
skip data at the granularity of index blocks.

The `GRANULARITY` parameter determines how many ANN sub-indexes are created. Bigger `GRANULARITY` values mean fewer but larger ANN
sub-indexes, up to the point where a column (or a column's data part) has only a single sub-index. In that case, the sub-index has a
"global" view of all column rows and can directly return all granules of the column (part) with relevant rows (there are at most 
`LIMIT [N]`-many such granules). In a second step, ClickHouse will load these granules and identify the actually best rows by performing a
brute-force distance calculation over all rows of the granules. With a small `GRANULARITY` value, each of the sub-indexes returns up to
`LIMIT N`-many granules. As a result, more granules need to be loaded and post-filtered. Note that the search accuracy is with both cases
equally good, only the processing performance differs. It is generally recommended to use a large `GRANULARITY` for ANN indexes and fall
back to a smaller `GRANULARITY` values only in case of problems like excessive memory consumption of the ANN structures. If no `GRANULARITY`
was specified for ANN indexes, the default value is 100 million.


# Available ANN Indexes

- [Annoy](/docs/en/engines/table-engines/mergetree-family/annindexes.md#annoy-annoy)

## Annoy {#annoy}

Annoy indexes are currently experimental, to use them you first need to `SET allow_experimental_annoy_index = 1`. They are also currently
disabled on ARM due to memory safety problems with the algorithm.

This type of ANN index implements [the Annoy algorithm](https://github.com/spotify/annoy) which is based on a recursive division of the
space in random linear surfaces (lines in 2D, planes in 3D etc.).

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/QkCCyLW0ehU"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

Syntax to create an Annoy index over an [Array](../../../sql-reference/data-types/array.md) column:

```sql
CREATE TABLE table
(
  id Int64,
  vectors Array(Float32),
  INDEX [ann_index_name] vectors TYPE annoy([Distance[, NumTrees]]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY id;
```

Syntax to create an ANN index over a [Tuple](../../../sql-reference/data-types/tuple.md) column:

```sql
CREATE TABLE table
(
  id Int64,
  vectors Tuple(Float32[, Float32[, ...]]),
  INDEX [ann_index_name] vectors TYPE annoy([Distance[, NumTrees]]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY id;
```

Annoy currently supports `L2Distance` and `cosineDistance` as distance function `Distance`. If no distance function was specified during
index creation, `L2Distance` is used as default. Parameter `NumTrees` is the number of trees which the algorithm creates (default if not
specified: 100). Higher values of `NumTree` mean more accurate search results but slower index creation / query times (approximately
linearly) as well as larger index sizes.

:::note
Indexes over columns of type `Array` will generally work faster than indexes on `Tuple` columns. All arrays **must** have same length. Use
[CONSTRAINT](/docs/en/sql-reference/statements/create/table.md#constraints) to avoid errors. For example, `CONSTRAINT constraint_name_1
CHECK length(vectors) = 256`.
:::

Setting `annoy_index_search_k_nodes` (default: `NumTrees * LIMIT`) determines how many tree nodes are inspected during SELECTs. Larger
values mean more accurate results at the cost of longer query runtime:

```sql
SELECT *
FROM table_name
ORDER BY L2Distance(vectors, Point)
LIMIT N
SETTINGS annoy_index_search_k_nodes=100;
```
