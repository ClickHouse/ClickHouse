-- A `NULL` or a `NaN` nested in a `Tuple` constant makes a comparison against that constant "not
-- true" for every row - `NULL` for a `NULL` element, false for a `NaN` one - while in key order the
-- constant has a definite position. A `NaN` nested in a `Tuple` key value is the mirror case: the
-- granule holding it cannot be proven wholly inside a comparison range. In both directions `count()`
-- has to agree with the rows the same predicate returns.

DROP TABLE IF EXISTS t_pk_tuple_nan_const;

CREATE TABLE t_pk_tuple_nan_const (t Tuple(Float64, Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 4;

INSERT INTO t_pk_tuple_nan_const VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((600.,1),0),((700.,1),0),((800.,1),0),((900.,1),0);

SELECT 'a NaN in the constant';
SELECT count() FROM t_pk_tuple_nan_const WHERE t <= (nan, 3);
SELECT count() FROM t_pk_tuple_nan_const WHERE t < (nan, 3);
SELECT count() FROM t_pk_tuple_nan_const WHERE t >= (nan, 3);
SELECT count() FROM t_pk_tuple_nan_const WHERE t = (nan, 3);
-- A folded computed constant reaches the index the same way.
SELECT count() FROM t_pk_tuple_nan_const WHERE t <= (sqrt(-1.), 3);
-- The trivial-count rewrite of the same predicate.
SELECT count() FROM (SELECT * FROM t_pk_tuple_nan_const WHERE t <= (nan, 3));
-- A real read of the same predicate, for comparison.
SELECT sum(x) FROM t_pk_tuple_nan_const WHERE t <= (nan, 3);

SELECT 'an ordinary constant still counts and prunes';
SELECT count() FROM t_pk_tuple_nan_const WHERE t <= (10., 1);
SELECT count() FROM t_pk_tuple_nan_const WHERE t >= (600., 1);

DROP TABLE t_pk_tuple_nan_const;

SELECT 'a NaN in the key data';

CREATE TABLE t_pk_tuple_nan_data (t Tuple(Float64, Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 4;

INSERT INTO t_pk_tuple_nan_data VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((nan,2),0),((nan,3),0),((nan,4),0),((nan,5),0);

SELECT count() FROM t_pk_tuple_nan_data WHERE t >= (5., 3);
SELECT count() FROM (SELECT * FROM t_pk_tuple_nan_data WHERE t >= (5., 3));
SELECT sum(x) FROM t_pk_tuple_nan_data WHERE t >= (5., 3);
SELECT count() FROM t_pk_tuple_nan_data WHERE t <= (5., 3);

DROP TABLE t_pk_tuple_nan_data;

SELECT 'a NULL in the constant, through a key transform';

CREATE TABLE t_pk_tuple_null_transform (t Tuple(Nullable(Float64), Float64)) ENGINE = MergeTree ORDER BY toString(t)
SETTINGS index_granularity = 1;

INSERT INTO t_pk_tuple_null_transform VALUES ((NULL,1)),((2,2)),((3,3));

SELECT count() FROM t_pk_tuple_null_transform WHERE t = (NULL, 1);
SELECT count() FROM t_pk_tuple_null_transform WHERE t != (NULL, 1);
SELECT count() FROM (SELECT * FROM t_pk_tuple_null_transform WHERE t != (NULL, 1));
SELECT count() FROM t_pk_tuple_null_transform WHERE t = (2, 2);

DROP TABLE t_pk_tuple_null_transform;

SELECT 'IN and NOT IN agree with the rows they return';

-- A set atom is a different path from a range atom: both the set index and the row-level `IN` compare
-- whole tuples by hash, so a nested `NULL` or `NaN` matches its identical copy on both sides, and an
-- unpacked set literal drops the element whose top-level `Nullable` column is `NULL` on both sides too.
-- Pin that `count()` keeps agreeing with the real read either way.

CREATE TABLE t_pk_tuple_null_in (t Tuple(Nullable(Int32), Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_null_in VALUES ((NULL,3),1),((1,3),1),((2,3),1);

SELECT count(), sum(x) FROM t_pk_tuple_null_in WHERE t IN ((NULL, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in WHERE t NOT IN ((NULL, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in WHERE t IN ((NULL, 3), (1, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in WHERE t NOT IN ((NULL, 3), (1, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in WHERE t IN (SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)'));
SELECT count(), sum(x) FROM t_pk_tuple_null_in WHERE t NOT IN (SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)'));

DROP TABLE t_pk_tuple_null_in;

CREATE TABLE t_pk_tuple_nan_in (t Tuple(Float64, Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1;

INSERT INTO t_pk_tuple_nan_in VALUES ((nan,3),1),((1.,3),1),((2.,3),1);

SELECT count(), sum(x) FROM t_pk_tuple_nan_in WHERE t IN ((nan, 3));
SELECT count(), sum(x) FROM t_pk_tuple_nan_in WHERE t NOT IN ((nan, 3));
SELECT count(), sum(x) FROM t_pk_tuple_nan_in WHERE t NOT IN ((nan, 3), (1., 3));

DROP TABLE t_pk_tuple_nan_in;

-- The same through a key transform and through partition pruning.

CREATE TABLE t_pk_tuple_null_in_transform (t Tuple(Nullable(Int32), Int32), x Int32) ENGINE = MergeTree ORDER BY toString(t)
SETTINGS index_granularity = 1;

INSERT INTO t_pk_tuple_null_in_transform VALUES ((NULL,3),1),((1,3),1),((2,3),1);

SELECT count(), sum(x) FROM t_pk_tuple_null_in_transform WHERE t IN ((NULL, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in_transform WHERE t NOT IN ((NULL, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in_transform WHERE t IN (SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)'));
SELECT count(), sum(x) FROM t_pk_tuple_null_in_transform WHERE t NOT IN (SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)'));

DROP TABLE t_pk_tuple_null_in_transform;

CREATE TABLE t_pk_tuple_null_in_partition (t Tuple(Nullable(Int32), Int32), x Int32) ENGINE = MergeTree PARTITION BY t ORDER BY x
SETTINGS allow_nullable_key = 1;

INSERT INTO t_pk_tuple_null_in_partition VALUES ((NULL,3),1),((1,3),1),((2,3),1);

SELECT count(), sum(x) FROM t_pk_tuple_null_in_partition WHERE t IN ((NULL, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in_partition WHERE t NOT IN ((NULL, 3));
SELECT count(), sum(x) FROM t_pk_tuple_null_in_partition WHERE t IN (SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)'));
SELECT count(), sum(x) FROM t_pk_tuple_null_in_partition WHERE t NOT IN (SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)'));

DROP TABLE t_pk_tuple_null_in_partition;

SELECT 'a NULL or a NaN nested inside a granule whose bounds are ordinary';

-- Key order puts `(2, NULL)` and `(2, nan)` between `(2, 1)` and `(3, 0)`, so the granule bounds stay
-- ordinary, while the row-level comparison of that row is `NULL` or false.

CREATE TABLE t_pk_tuple_null_inside (t Tuple(Int32, Nullable(Int32)), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 3, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_null_inside VALUES ((2,1),1),((2,NULL),1),((3,0),1);

SELECT count() FROM t_pk_tuple_null_inside WHERE t >= (2, 0);
SELECT count() FROM (SELECT * FROM t_pk_tuple_null_inside WHERE t >= (2, 0));
SELECT sum(x) FROM t_pk_tuple_null_inside WHERE t >= (2, 0);

DROP TABLE t_pk_tuple_null_inside;

CREATE TABLE t_pk_tuple_nan_inside (t Tuple(Int32, Float64), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 3;

INSERT INTO t_pk_tuple_nan_inside VALUES ((2,1.),1),((2,nan),1),((3,0.),1);

SELECT count() FROM t_pk_tuple_nan_inside WHERE t >= (2, 0.);
SELECT count() FROM (SELECT * FROM t_pk_tuple_nan_inside WHERE t >= (2, 0.));
SELECT sum(x) FROM t_pk_tuple_nan_inside WHERE t >= (2, 0.);

DROP TABLE t_pk_tuple_nan_inside;

SELECT 'a range bounded above by an ordinary value excludes a nested NULL or NaN on both sides';

-- Key order puts `(2, NULL)` and `(2, nan)` above `(2, 5)`, and the row-level comparison against `(2, 5)`
-- is `NULL` or false, so such a range keeps its exactness and still agrees with the rows.

CREATE TABLE t_pk_tuple_null_upper (t Tuple(Int32, Nullable(Int32)), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 3, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_null_upper VALUES ((2,1),1),((2,NULL),1),((3,0),1),((4,NULL),1),((4,4),1),((5,1),1);

SELECT count(), sum(x) FROM t_pk_tuple_null_upper WHERE t <= (2, 5);
SELECT count(), sum(x) FROM t_pk_tuple_null_upper WHERE t <= (4, 5);
SELECT count(), sum(x) FROM t_pk_tuple_null_upper WHERE t != (2, 1);

DROP TABLE t_pk_tuple_null_upper;

CREATE TABLE t_pk_tuple_nan_upper (t Tuple(Int32, Float64), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 3;

INSERT INTO t_pk_tuple_nan_upper VALUES ((2,1.),1),((2,nan),1),((3,0.),1),((4,nan),1),((4,4.),1),((5,1.),1);

SELECT count(), sum(x) FROM t_pk_tuple_nan_upper WHERE t <= (2, 5.);
SELECT count(), sum(x) FROM t_pk_tuple_nan_upper WHERE t <= (4, 5.);
SELECT count(), sum(x) FROM t_pk_tuple_nan_upper WHERE t != (2, 1.);

DROP TABLE t_pk_tuple_nan_upper;

SELECT 'a NULL in the key data, hidden by a key transform';

-- `toString` maps `(NULL, 1)` to the ordinary key string `'(NULL,1)'`, which the index compares as unequal
-- to `'(2,1)'`, while the row-level comparison is `NULL`. The key type is a plain `String`, so the atom
-- itself must give up its exactness. The same holds for `assumeNotNull` over a top-level `NULL`.

CREATE TABLE t_pk_tuple_null_data_transform (t Tuple(Nullable(Int32), Int32), x Int32) ENGINE = MergeTree ORDER BY toString(t)
SETTINGS index_granularity = 1;

INSERT INTO t_pk_tuple_null_data_transform VALUES ((NULL,1),1),((2,1),1),((3,1),1);

SELECT count() FROM t_pk_tuple_null_data_transform WHERE t != (2, 1);
SELECT count(), sum(x) FROM t_pk_tuple_null_data_transform WHERE t != (2, 1);
SELECT count() FROM t_pk_tuple_null_data_transform WHERE NOT (t = (2, 1));
SELECT count(), sum(x) FROM t_pk_tuple_null_data_transform WHERE NOT (t = (2, 1));
SELECT count() FROM t_pk_tuple_null_data_transform WHERE t = (2, 1);

DROP TABLE t_pk_tuple_null_data_transform;

CREATE TABLE t_pk_null_data_assume_not_null (n Nullable(Int32), x Int32) ENGINE = MergeTree ORDER BY assumeNotNull(n)
SETTINGS index_granularity = 1;

INSERT INTO t_pk_null_data_assume_not_null VALUES (NULL,1),(2,1),(3,1);

SELECT count() FROM t_pk_null_data_assume_not_null WHERE n != 2;
SELECT count(), sum(x) FROM t_pk_null_data_assume_not_null WHERE n != 2;

DROP TABLE t_pk_null_data_assume_not_null;

-- A `NULL` nested in an `Array` or a `Map` is compared as an ordinary value, so the key analysis of
-- these keys stays exact; the counts must agree with the row-level filter.
CREATE TABLE t_pk_array_nested_null (a Array(Nullable(Int32)), x Int32) ENGINE = MergeTree ORDER BY a
SETTINGS allow_nullable_key = 1, index_granularity = 2;

INSERT INTO t_pk_array_nested_null SELECT if(number % 3 = 0, [NULL], [number::Int32]), 1 FROM numbers(20);

SELECT count() FROM t_pk_array_nested_null WHERE a = [NULL];
SELECT count(), sum(x) FROM t_pk_array_nested_null WHERE a = [NULL];
SELECT count() FROM t_pk_array_nested_null WHERE a != [NULL];
SELECT count(), sum(x) FROM t_pk_array_nested_null WHERE a != [NULL];

DROP TABLE t_pk_array_nested_null;

CREATE TABLE t_pk_map_nested_null_transform (m Map(String, Array(Nullable(Float64))), x Int32) ENGINE = MergeTree
ORDER BY toString(m) SETTINGS index_granularity = 2;

INSERT INTO t_pk_map_nested_null_transform SELECT map('k', if(number % 3 = 0, [NULL], [number::Float64])), 1 FROM numbers(20);

SELECT count() FROM t_pk_map_nested_null_transform WHERE m = map('k', [NULL]);
SELECT count(), sum(x) FROM t_pk_map_nested_null_transform WHERE m = map('k', [NULL]);
SELECT count() FROM t_pk_map_nested_null_transform WHERE m != map('k', [NULL]);
SELECT count(), sum(x) FROM t_pk_map_nested_null_transform WHERE m != map('k', [NULL]);

DROP TABLE t_pk_map_nested_null_transform;

SELECT 'IS NOT DISTINCT FROM treats a nested NULL as an ordinary value';

-- `<=>` compares a `Tuple` element-wise in the null-safe way, so `(NULL, 1) <=> (NULL, 1)` is true, and
-- the point atom stays usable for pruning, while `count()` keeps agreeing with the real read.

CREATE TABLE t_pk_tuple_null_safe (t Tuple(Nullable(Int32), Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_null_safe VALUES ((0,0),1),((NULL,1),1),((5,1),1),((NULL,2),1);

SELECT count(), sum(x) FROM t_pk_tuple_null_safe WHERE t <=> (NULL, 1);
SELECT count(), sum(x) FROM t_pk_tuple_null_safe WHERE NOT (t <=> (NULL, 1));
SELECT count(), sum(x) FROM t_pk_tuple_null_safe WHERE t <=> (5, 1);
SELECT count() FROM t_pk_tuple_null_safe WHERE t <=> (NULL, 1) SETTINGS force_primary_key = 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT x FROM t_pk_tuple_null_safe WHERE t <=> (NULL, 1) SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Condition:%';

DROP TABLE t_pk_tuple_null_safe;

CREATE TABLE t_pk_tuple_null_safe_transform (t Tuple(Nullable(Int32), Int32), x Int32) ENGINE = MergeTree ORDER BY toString(t)
SETTINGS index_granularity = 1;

INSERT INTO t_pk_tuple_null_safe_transform VALUES ((0,0),1),((NULL,1),1),((5,1),1),((NULL,2),1);

SELECT count(), sum(x) FROM t_pk_tuple_null_safe_transform WHERE t <=> (NULL, 1);
SELECT count(), sum(x) FROM t_pk_tuple_null_safe_transform WHERE NOT (t <=> (NULL, 1));
SELECT count() FROM t_pk_tuple_null_safe_transform WHERE t <=> (NULL, 1) SETTINGS force_primary_key = 1;

DROP TABLE t_pk_tuple_null_safe_transform;

SELECT 'A NaN nested in an Array is ordered the same way at row level and in the key';

-- An `Array` comparison treats two `NaN` elements as equal and orders a `NaN` after every number,
-- exactly like `compareAt` in the index, so atoms on `[nan]` stay exact and agree with the real read.

CREATE TABLE t_pk_array_nan (a Array(Float64), x Int32) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1;

INSERT INTO t_pk_array_nan VALUES ([nan],1),([1],1),([2],1),([nan, 1],1),([0],1);

SELECT count(), sum(x) FROM t_pk_array_nan WHERE a = [nan];
SELECT count(), sum(x) FROM t_pk_array_nan WHERE a != [nan];
SELECT count(), sum(x) FROM t_pk_array_nan WHERE a <= [nan];
SELECT count(), sum(x) FROM t_pk_array_nan WHERE a > [1];
SELECT count(), sum(x) FROM t_pk_array_nan WHERE NOT (a < [1]);
-- `[nan] <=> [nan]` is true, so `<=>` matches the rows that hold `[nan]`.
SELECT count(), sum(x) FROM t_pk_array_nan WHERE a <=> [nan];

DROP TABLE t_pk_array_nan;

CREATE TABLE t_pk_tuple_array_nan (t Tuple(Array(Float64), UInt8), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1;

INSERT INTO t_pk_tuple_array_nan VALUES (([nan], 1),1),(([1], 1),1),(([nan], 2),1),(([0], 0),1),(([nan, 1], 1),1);

SELECT count(), sum(x) FROM t_pk_tuple_array_nan WHERE t = ([nan], 1);
SELECT count(), sum(x) FROM t_pk_tuple_array_nan WHERE t != ([nan], 1);
SELECT count(), sum(x) FROM t_pk_tuple_array_nan WHERE t <= ([nan], 1);
SELECT count(), sum(x) FROM t_pk_tuple_array_nan WHERE t > ([1], 5);
SELECT count(), sum(x) FROM t_pk_tuple_array_nan WHERE NOT (t < ([nan], 1));
SELECT count(), sum(x) FROM t_pk_tuple_array_nan WHERE t <=> ([nan], 1);

DROP TABLE t_pk_tuple_array_nan;
