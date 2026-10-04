-- A boundary mark of a `Tuple` key column can hold a `NULL` inside the tuple. The key stores NULLs
-- last, while `Field` order puts them below every value, so such a mark used to compare below the mark
-- before it: the granule between them looked empty and the rows it holds were never read.

DROP TABLE IF EXISTS t_pk_tuple_nullable_element;

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(Nullable(Float64), Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 4, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((NULL,2),0),((NULL,3),0),((NULL,4),0),((NULL,5),0);

SELECT 'a NULL in a later granule';
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (5., 3);
SELECT t FROM t_pk_tuple_nullable_element WHERE t >= (5., 3) ORDER BY t;
-- The implicit projection is off here: a granule whose bound holds a NULL nested in the tuple cannot
-- be claimed to match a comparison wholly, which is a separate defect of the range algebra itself.
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (5., 3) SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (10., 1);

-- The same counts without the primary key. The condition cache is off here: it is keyed by the
-- condition, so a cached verdict from the queries above would answer these instead of a real read.
SELECT 'without the primary key';
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (5., 3) SETTINGS use_primary_key = 0, use_query_condition_cache = 0;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (5., 3) SETTINGS use_primary_key = 0, use_query_condition_cache = 0;

DROP TABLE t_pk_tuple_nullable_element;

SELECT 'a NULL in the first granule';

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(Nullable(Float64), Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 2, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((NULL,2),0),((NULL,3),0);

SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (2., 1);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t IN ((10., 1), (500., 7));

DROP TABLE t_pk_tuple_nullable_element;

-- The same key column also feeds the partition key, so the part's partition-minmax bound of `t` is
-- consulted by the analysis too. The bound is built per element with the NULLs filtered out, so it is
-- ordered and holds every value without a NULL: it can prune nothing that a comparison matches.
SELECT 'the key column in the partition key';

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(Nullable(Float64), Int32), x Int32) ENGINE = MergeTree
PARTITION BY t.2 >= 0 ORDER BY t
SETTINGS index_granularity = 4, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((NULL,2),0),((NULL,3),0),((NULL,4),0),((NULL,5),0);

SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (5., 3) SETTINGS use_partition_minmax_for_primary_key_pruning = 1;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (5., 3) SETTINGS use_partition_minmax_for_primary_key_pruning = 1, optimize_use_implicit_projections = 0;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (10., 1) SETTINGS use_partition_minmax_for_primary_key_pruning = 1;

DROP TABLE t_pk_tuple_nullable_element;

-- Here `t` is a suffix key column that the in-memory index drops, so its partition-minmax bound is the
-- only information the analysis has about it.
SELECT 'the key column in the partition key, not loaded in memory';

CREATE TABLE t_pk_tuple_nullable_element (id UInt64, t Tuple(Nullable(Float64), Int32), x Int32) ENGINE = MergeTree
PARTITION BY t.2 >= 0 ORDER BY (id, t)
SETTINGS index_granularity = 4, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.5;

INSERT INTO t_pk_tuple_nullable_element VALUES (1,(1.,1),0),(2,(2.,1),0),(3,(10.,1),1),(4,(500.,7),1),(5,(NULL,2),0),(6,(NULL,3),0),(7,(NULL,4),0),(8,(NULL,5),0);

SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (5., 3) SETTINGS use_partition_minmax_for_primary_key_pruning = 1;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (5., 3) SETTINGS use_partition_minmax_for_primary_key_pruning = 1;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (10., 1) SETTINGS use_partition_minmax_for_primary_key_pruning = 1;

DROP TABLE t_pk_tuple_nullable_element;

-- The NULL is in a later element of the tuple, after an element that differs between the two marks of
-- the granule. The pair of marks is ordered in `Field` order, yet `(2, NULL)` comes out below `(2, 3)`,
-- which the key stores before it.
SELECT 'a NULL in a later element';

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(UInt8, Nullable(UInt8)), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 2, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((1,5),0),((2,3),0),((2,NULL),0),((3,1),0),((3,NULL),0),((4,1),0);

SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (2, 3);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (2, 3) AND t <= (2, 200);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (3, 1);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (2, 3) SETTINGS use_primary_key = 0, use_query_condition_cache = 0;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (2, 3);

DROP TABLE t_pk_tuple_nullable_element;

-- On a descending key column the upper bound of a granule is its first mark.
SELECT 'a descending key column';

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(UInt8, Nullable(UInt8)), x Int32) ENGINE = MergeTree ORDER BY t DESC
SETTINGS index_granularity = 2, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((4,1),0),((3,NULL),0),((3,1),0),((2,NULL),0),((2,3),0),((1,5),0);

SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (2, 3);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (3, 1);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (3, 1);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (3, 1) SETTINGS use_primary_key = 0, use_query_condition_cache = 0;

DROP TABLE t_pk_tuple_nullable_element;
