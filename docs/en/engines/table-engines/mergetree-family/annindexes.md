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

The queries are expensive because the L2 (Euclidean) distance between `Point` and all points in `column` and must be computed. To speed this process up, Approximate Nearest Neighbor Search Indexes (ANN indexes) store a compact representation of the search space (using clustering, search trees, etc.) which allows to compute an approximate answer quickly.

# Creating ANN Indexes

As long as ANN indexes are experimental, you first need to `SET allow_experimental_annoy_index = 1`.

Syntax to create an ANN index over an `Array` column:

```sql
CREATE TABLE table
(
  `id` Int64,
  `embedding` Array(Float32),
  INDEX <ann_index_name> embedding TYPE <ann_index_type>(<ann_index_parameters>) GRANULARITY <N>
)
ENGINE = MergeTree
ORDER BY id;
```

Syntax to create an ANN index over a `Tuple` column:

```sql
CREATE TABLE table
(
  `id` Int64,
  `embedding` Tuple(Float32[, Float32[, ...]]),
  INDEX <ann_index_name> embedding TYPE <ann_index_type>(<ann_index_parameters>) GRANULARITY <N>
)
ENGINE = MergeTree
ORDER BY id;
```

ANN indexes are built during column insertion and merge and `INSERT` and `OPTIMIZE` statements will be slower than for ordinary tables. ANNIndexes are ideally used only with immutable or rarely changed data, respectively there are much more read requests than write requests.

Similar to regular skip indexes, ANN indexes are constructed over granules and each indexed block consists of `GRANULARITY = <N>`-many
granules. For example, if the primary index granularity of the table is 8192 (setting `index_granularity = 8192`) and `GRANULARITY = 2`,
then each indexed block will consist of 16384 rows. However, unlike skip indexes, ANN indexes are not only able to skip the entire indexed
block, they are able to skip individual granules in indexed blocks. As a result, the `GRANULARITY` parameter has a different meaning in ANN
indexes than in normal skip indexes. Basically, the bigger `GRANULARITY` is chosen, the more data is provided to a single ANN index, and the
higher the chance that with the right hyper parameters, the index will remember the data structure better.

# Using ANN Indexes

ANN indexes support two types of queries:

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
  FROM table
  [WHERE ...]
  ORDER BY DistanceFunction(column, Point)
  LIMIT N
  ```

`DistanceFunction` is a [distance function](/docs/en/sql-reference/functions/distance-functions.md), `Point` is a reference vector (e.g. `(0.17, 0.33, ...)`) and `MaxDistance` is a floating point value which restricts the size of the neighbourhood.

:::tip
To avoid writing out large vectors, you can use [query parameters](/docs/en//interfaces/cli.md#queries-with-parameters-cli-queries-with-parameters), e.g.

```bash
clickhouse-client --param_vec='hello' --query="SELECT * FROM table WHERE L2Distance(embedding, {vec: Array(Float32)}) < 1.0"
```
:::

ANN indexes cannot speed up queries that contain both a `WHERE DistanceFunction(column, Point) < MaxDistance` and an `ORDER BY DistanceFunction(column, Point)` clause. Also, the approximate algorithms used to determine the nearest neighbors require a limit, hence queries that use an ANN index must have a `LIMIT` clause.

An ANN index is only used if the query has a `LIMIT` value smaller than setting `max_limit_for_ann_queries` (default: 1 million rows). This is a safety measure which helps to avoid large memory consumption by external libraries for approximate neighbor search.

# Available ANN Indexes

- [Annoy](/docs/en/engines/table-engines/mergetree-family/annindexes.md#annoy-annoy)

## Annoy {#annoy}

(currently disabled on ARM due to memory safety problems with the algorithm)

This type of ANN index implements [the Annoy algorithm](https://github.com/spotify/annoy) which uses a recursive division of the space in random linear surfaces (lines in 2D, planes in 3D etc.).

Syntax to create a Annoy index over a `Array` column:

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

Syntax to create a Annoy index over a `Tuple` column:

```sql
CREATE TABLE table
(
  id Int64,
  embedding Tuple(Float32[, Float32[, ...]]),
  INDEX <ann_index_name> embedding TYPE annoy([DistanceName[, NumTrees]]) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

Parameter `DistanceName` is name of a distance function (default `L2Distance`). Annoy currently supports `L2Distance` and `cosineDistance` as distance functions. Parameter `NumTrees` (default: 100) is the number of trees which the algorithm will create. Higher values of `NumTree` mean slower `CREATE` and `SELECT` statements (approximately linearly), but increase the accuracy of search results.

:::note
Indexes over columns of type `Array` will generally work faster than indexes on `Tuple` columns. All arrays **must** have same length. Use [CONSTRAINT](/docs/en/sql-reference/statements/create/table.md#constraints) to avoid errors. For example, `CONSTRAINT constraint_name_1 CHECK length(embedding) = 256`.
:::

Setting `annoy_index_search_k_nodes` (default: `NumTrees * LIMIT`) determines how many tree nodes are inspected during SELECTs. It can be used to
balance runtime and accuracy at runtime.

Example:

``` sql
SELECT *
FROM table_name [WHERE ...]
ORDER BY L2Distance(column, Point)
LIMIT N
SETTINGS annoy_index_search_k_nodes=100
```
