# Approximate Nearest Neighbor Search Indexes [experimental] {#table_engines-ANNIndex}

The main task that indexes achieve is to quickly find nearest neighbors for multidimensional data. An example of such a problem can be finding similar pictures (texts) for a given picture (text). That problem can be reduced to finding the nearest [embeddings](https://cloud.google.com/architecture/overview-extracting-and-serving-feature-embeddings-for-machine-learning). They can be created from data using [UDF](../../../sql-reference/functions/index.md#executable-user-defined-functions).

The next query finds the closest neighbors in N-dimensional space using the L2 (Euclidean) distance:
``` sql 
SELECT * 
FROM table_name 
WHERE L2Distance(Column, Point) < MaxDistance 
LIMIT N
```
But it will take some time for execution because of the long calculation of the distance between `TargetEmbedding` and all other vectors. This is where ANN indexes can help. They store a compact approximation of the search space (e.g. using clustering, search trees, etc.) and are able to compute approximate neighbors quickly.

## Indexes Structure

Approximate Nearest Neighbor Search Indexes (`ANNIndexes`) are similar to skip indexes. They are constructed by some granules and determine which of them should be skipped. Compared to skip indices, ANN indices use their results not only to skip some group of granules, but also to select particular granules from a set of granules.

`ANNIndexes` are designed to speed up two types of queries:

- ######  Type 1: Where 
   ``` sql 
   SELECT * 
   FROM table_name 
   WHERE DistanceFunction(Column, Point) < MaxDistance 
   LIMIT N
   ```
- ###### Type 2: Order by
  ``` sql
  SELECT * 
  FROM table_name [WHERE ...] 
  ORDER BY DistanceFunction(Column, Point) 
  LIMIT N
  ```

In these queries, `DistanceFunction` is selected from [distance functions](../../../sql-reference/functions/distance-functions). `Point` is a known vector (something like `(0.1, 0.1, ... )`). `Value` - a float value that will bound the neighbourhood.

!!! note "Note"
    ANN index can't speed up query that satisfies both types(`where + order by`, only one of them). All queries must have the limit, as algorithms are used to find nearest neighbors and need a specific number of them.

Both types of queries are handled the same way. The indexes get `n` neighbors (where `n` is taken from the `LIMIT` clause) and work with them. In `ORDER BY` query they remember the numbers of all parts of the granule that have at least one of neighbor. In `WHERE` query they remember only those parts that satisfy the requirements.

## Create table with ANNIndex

```sql
CREATE TABLE t
(
  `id` Int64,
  `number` Tuple(Float32, Float32, Float32),
  INDEX x number TYPE annoy GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

```sql
CREATE TABLE t
(
  `id` Int64,
  `number` Array(Float32),
  INDEX x number TYPE annoy GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

With greater `GRANULARITY` indexes remember the data structure better. The `GRANULARITY` indicates how many granules will be used to construct the index. The more data is provided for the index, the more of it can be handled by one index and the more chances that with the right hyperparameters the index will remember the data structure better. But some indexes can't be built if they don't have enough data, so this granule will always participate in the query. For more information, see the description of indexes.

As the indexes are built only during insertions into table, `INSERT` and `OPTIMIZE` queries are slower than for ordinary table. At this stage indexes remember all the information about the given data. ANNIndexes should be used if you have immutable or rarely changed data and many read requests.
    
You can create your table with index which uses certain algorithm. Now only indices based on the following algorithms are supported:

# Index list
- [Annoy](../../../engines/table-engines/mergetree-family/annindexes.md#annoy-annoy)

# Annoy {#annoy}
Implementation of the algorithm was taken from [this repository](https://github.com/spotify/annoy).

Short description of the algorithm:
The algorithm recursively divides in half all space by random linear surfaces (lines in 2D, planes in 3D e.t.c.). Thus it makes tree of polyhedrons and points that they contains. Repeating the operation several times for greater accuracy it creates a forest.
To find K Nearest Neighbours it goes down through the trees and fills the buffer of closest points using the priority queue of polyhedrons. Next, it sorts buffer and return the nearest K points.

__Examples__:
```sql
CREATE TABLE t
(
  id Int64,
  number Tuple(Float32, Float32, Float32),
  INDEX x number TYPE annoy(T) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```

```sql
CREATE TABLE t
(
  id Int64,
  number Array(Float32),
  INDEX x number TYPE annoy(T) GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```
!!! note "Note"
    Table with array field will work faster, but all arrays **must** have same length. Use [CONSTRAINT](../../../sql-reference/statements/create/table.md#constraints) to avoid errors. For example, `CONSTRAINT constraint_name_1 CHECK length(number) = 256`.

Parameter `T` is the number of trees which algorithm will create. The bigger it is, the slower (approximately linear) it works (in both `CREATE` and `SELECT` requests), but the better accuracy you get (adjusted for randomness). 

Annoy supports only `L2Distance`.

In the `SELECT` in the settings (`ann_index_select_query_params`) you can specify the size of the internal buffer (more details in the description above or in the [original repository](https://github.com/spotify/annoy)). During the query it will inspect up to `search_k` nodes which defaults to `n_trees * n` if not provided. `search_k` gives you a run-time tradeoff between better accuracy and speed.

__Example__:
``` sql
SELECT * 
FROM table_name [WHERE ...] 
ORDER BY L2Distance(Column, Point) 
LIMIT N
SETTING ann_index_select_query_params=`k_search=100`
```
