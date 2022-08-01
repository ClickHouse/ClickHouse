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
rschu1ze marked this conversation as resolved.
Show resolved

  You can create your table with index which uses certain algorithm. Now only indices based on the following algorithms are supported:

  # Index list
 - [HNSW](../../../engines/table-engines/mergetree-family/annindexes.md#hnsw---hnsw)

  
## HNSW { # HNSW}
- `hnsw([M, efConstruction, graph_degree, maxM, maxM0])` -- Stores and operates with Hierarchical ANN search graph built by [Hnsw algortihm](https://ieeexplore.ieee.org/document/8594636). The graph construction algorithm, as well as its structure, very much resembles the Skip list data structure. Short parameters description: 
    - `M` — number of established connections for every vertex on any level except zero level. A reasonable range for this parameter is from `5` to `48`. Bigger `M` is better for high recall and/or high dimensional data. The parameter also defines the memory consumption of the algorithm (which is proportional to M). Default value: `16`
    - `efConstruction` — size of the dynamic candidate list. Construction speed/index quality tradeoff is controlled via this parameter. Increasing the value of `efConstruction` improves the quality of a constructed graph and leads to higher accuracy of search. However this also leads to longer indexing times. Default value: `200`.
    - `maxM`  — maximum number of connections for each element per layer. Controls the density of graph on each level except the last one. Default value: `M`.
    - `maxM0` — maximum number of connections for each element on last layer.  Default value: `2 * M`.

- Implementation of the algorithm was taken from [nmslib library](https://github.com/nmslib/nmslib).