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
