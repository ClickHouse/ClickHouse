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
