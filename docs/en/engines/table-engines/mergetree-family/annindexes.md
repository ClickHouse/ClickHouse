# Approximate Nearest Neighbor Search Indexes [experimental] {#table_engines-ANNIndex}

The main task that indexes help to solve is to find the nearest neighbors for multidimensional data. An example of such a problem could be similar pictures or texts, for which the problem is reduced to finding the nearest [embeddings](https://cloud.google.com/architecture/overview-extracting-and-serving-feature-embeddings-for-machine-learning). They can be created from data using [UDF](../../../sql-reference/functions/index.md#executable-user-defined-functions).

## Indexes Structure

Approximate Nearest Neighbor Search Indexes (`ANNIndexes`) are simmilar to skip indexes. They are constructed by some granules and determine which of them should be skipped. Compared to skip indices, ANN indices use their results not only to skip some group of granules, but also to select particular granules from a set of granules.

`ANNIndexes` are designed to speed up two types of queries:

- ######  Type 1: Where 
   ``` sql 
   SELECT * 
   FROM table_name 
   WHERE DistanceFunction(Column, TargetVector) < Value 
   LIMIT N
   ```
- ###### Type 2: Order by
  ``` sql
  SELECT * 
  FROM table_name [WHERE ...] 
  ORDER BY DistanceFunction(Column, TargetVector) 
  LIMIT N
  ```

In these queries, `DistanceFunction` is selected from tuples of distance functions. `TargetVector` is a known embedding (something like `(0.1, 0.1, ... )`). `Value` - a float value that will bound the neighbourhood.

!!! note "Note"
    ANNIndex can't speed up query that satisfies both types and they work only for Tuples. All queries must have the limit, as algorithms are used to find nearest neighbors and need a specific number of them.

Both types of queries are handled the same way. The indexes get `n` neighbors (where `n` is taken from the `LIMIT` section) and work with them. In `ORDER BY` query they remember the numbers of all parts of the granule that have at least one of neighbor. In `WHERE` query they remember only those parts that satisfy the requirements.

###### Create table with ANNIndex
```
CREATE TABLE t
(
  `id` Int64,
  `number` Tuple(Float32, Float32, Float32),
  INDEX x number TYPE annoy GRANULARITY N
)
ENGINE = MergeTree
ORDER BY id;
```
    
Number of granules in granularity should be large. With greater `GRANULARITY` indexes remember the data structure better. But some indexes can't be built if they don't have enough data, so this granule will always participate in the query. For more information, see the description of indexes.

As the indexes are built only during insertions into table, `INSERT` and `OPTIMIZE` queries are slower than for ordinary table. At this stage indexes remember all the information about the given data. ANNIndexes should be used if you have immutable or rarely changed data and many read requests.
    
You can create your table with index which uses certain algorithm. Now only indices based on the following algorithms are supported:

# Index list
- [Annoy](../../../engines/table-engines/mergetree-family/annindexes.md#annoy-annoy)

# Annoy {#annoy}
Implementation of the algorithm was taken from [this repository](https://github.com/spotify/annoy).

Short description of the algorithm:
The algorithm recursively divides in half all space by random linear surfaces (lines in 2D, planes in 3D e.t.c.). Thus it makes tree of polyhedrons and points that they contains. Repeating the operation several times for greater accuracy it creates a forest.
To find K Nearest Neighbours it goes down through the trees and fills the buffer of closest points using the priority queue of polyhedrons. Next, it sorts buffer and return the nearest K points.

__Example__:
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
Parameter `T` is the number of trees which algorithm will create. The bigger it is, the slower (approximately linear) it works (in both `CREATE` and `SELECT` requests), but the better accuracy you get (adjusted for randomness).

In the `SELECT` in the settings (`ann_index_params`) you can specify the size of the internal buffer (more details in the description above or in the [original repository](https://github.com/spotify/annoy)).
This parameter may help you to adjust the trade-off between query speed and accuracy.