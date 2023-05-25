# Approximate Nearest Neighbor Search Indexes [experimental] {#table_engines-ANNIndex}

Nearest neighborhood search refers to the problem of finding the point(s) with the smallest distance to a given point in an n-dimensional
space. Since exact search is in practice usually typically too slow, the task is often solved with approximate algorithms. A popular use
case of of neighbor search is finding similar pictures (texts) for a given picture (text). Pictures (texts) can be decomposed into
[embeddings](https://cloud.google.com/architecture/overview-extracting-and-serving-feature-embeddings-for-machine-learning), and instead of
comparing pictures (texts) pixel-by-pixel (character-by-character), only the embeddings are compared.

In terms of SQL, the problem can be expressed as follows:

``` sql
SELECT *
FROM table
WHERE L2Distance(column, Point) < MaxDistance
LIMIT N
```

``` sql
SELECT *
FROM table
ORDER BY L2Distance(column, Point)
LIMIT N
```

The queries are expensive because the L2 distance (Euclidean distance) between all points in `column` and `Point` must be computed. To speed this process up, ANN indexes store a compact representation of the search space (using clustering, search trees, etc.) which allows to compute an approximate answer quickly.

## Indexes Structure

Approximate Nearest Neighbor Search Indexes (or `ANNIndexes`) are similar to skip indexes. They are constructed over granules and determine which granules can be skipped. Compared to skip indices, ANN indices are not only able to skip granules, they can also to select particular granules from a set of granules.

`ANNIndexes` support two types of queries:

- WHERE queries:
   ``` sql
   SELECT *
   FROM table
   WHERE DistanceFunction(column, Point) < MaxDistance
   LIMIT N
   ```

- ORDER BY queries:
  ``` sql
  SELECT *
  FROM table [WHERE ...]
  ORDER BY DistanceFunction(column, Point)
  LIMIT N
  ```

`DistanceFunction` is a [distance functions](/docs/en/sql-reference/functions/distance-functions.md), `Point` is a given vector (e.g. `(0.17, 0.33, ...)`) and `MaxDistance` is a float value which restricts the size of the neighbourhood.

To avoid writing large vectors, you can also use [query parameters](/docs/en//interfaces/cli.md#queries-with-parameters-cli-queries-with-parameters), e.g.

```bash
clickhouse-client --param_vec='hello' --query="SELECT * FROM table WHERE L2Distance(embedding, {vec: Array(Float32)}) < 1.0"
```

ANN index cannot speed up query that contain both `WHERE` and `ORDER BY`. Queries must have a limit, as the approximate algorithms used to determine the nearest neighbors require a specific number of them.

Indexes are only used for queries with a `LIMIT` value smaller than setting `max_limit_for_ann_queries` (default: 1 million rows). This helps to prevent memory overflows in queries with a large limit.

Both types of queries are processed similarly. The indexes are passed the number of neighbors `N`. In `ORDER BY` query they remember the numbers of all parts of the granule that have at least one of neighbor. In `WHERE` query they remember only those parts that satisfy the requirements.


## Creating Tables with an ANN Index

As long as ANN indexes are experimental, you first need to `SET allow_experimental_annoy_index = 1`.

Syntax:

```sql
CREATE TABLE table
(
  `id` Int64,
  `embedding` Tuple(Float32, Float32, Float32),
  INDEX <ann_index_name> embedding TYPE <ann_index_type>(<ann_index_parameters>) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

```sql
CREATE TABLE table
(
  `id` Int64,
  `embedding` Array(Float32),
  INDEX <ann_index_name> embedding TYPE <ann_index_type>(<ann_index_parameters>) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

With greater `GRANULARITY` indexes remember the data structure better. The `GRANULARITY` indicates how many granules will be used to construct the index. The more data is provided for the index, the more of it can be handled by one index and the more chances that with the right hyperparameters the index will remember the data structure better. But some indexes can't be built if they don't have enough data, so this granule will always participate in the query. For more information, see the description of indexes.

Note that ANN indexes are built during column insertion and merge, i.e. `INSERT` and `OPTIMIZE` statements are slower than for ordinary tables. ANNIndexes are ideally used only with immutable or rarely changing data in conjunction with many read requests.

# Index list

- [Annoy](/docs/en/engines/table-engines/mergetree-family/annindexes.md#annoy-annoy)

# Annoy {#annoy}

(currently disabled on ARM due to problems with the algorithm)

This ANN index type implements [Annoy indexes](https://github.com/spotify/annoy).

Short description of the algorithm:
The algorithm recursively divides in half all space by random linear surfaces (lines in 2D, planes in 3D etc.). Thus it makes tree of polyhedrons and points that they contains. Repeating the operation several times for greater accuracy it creates a forest.
To find K Nearest Neighbours it goes down through the trees and fills the buffer of closest points using the priority queue of polyhedrons. Next, it sorts buffer and return the nearest K points.

Examples:

```sql
CREATE TABLE table
(
  id Int64,
  embedding Tuple(Float32, Float32, Float32),
  INDEX <ann_index_name> embedding TYPE annoy([DistanceName[, NumTrees]]) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

```sql
CREATE TABLE table
(
  id Int64,
  embedding Array(Float32),
  INDEX <ann_index_name> embedding TYPE annoy([DistanceName[, NumTrees]]) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

:::note
Indexes over columns of type `Array` will generally work faster than indexes on `Tuple` columns. All arrays **must** have same length. Use [CONSTRAINT](/docs/en/sql-reference/statements/create/table.md#constraints) to avoid errors. For example, `CONSTRAINT constraint_name_1 CHECK length(embedding) = 256`.
:::

Parameter `DistanceName` is name of a distance function with default `L2Distance`. Parameter `NumTrees` (default: 100) is the number of trees which the algorithm will create. Higher values of `NumTree` mean slower `CREATE` and `SELECT` statements (approximately linearly), but increase the accuracy of search results.

```sql
CREATE TABLE table
(
  id Int64,
  embedding Array(Float32),
  INDEX ann_index_name embedding TYPE annoy('cosineDistance') GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

Annoy currently supports `L2Distance` and `cosineDistance` as distance functions.

Setting `annoy_index_search_k_nodes` (default: `NumTrees * LIMIT`) determines how many nodes are inspected during SELECTs. It can be used to
balance runtime and accuracy at runtime.

Example:

``` sql
SELECT *
FROM table_name [WHERE ...]
ORDER BY L2Distance(column, Point)
LIMIT N
SETTINGS annoy_index_search_k_nodes=100
```
