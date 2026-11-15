-- A NaN inside a Tuple orders above only the values that share its prefix, so a granule can span one
-- while neither of its bounds holds one, and every row comparison against it is false. Exact counting
-- must therefore not read such a granule as wholly matching.

DROP TABLE IF EXISTS t_nan_tuple_key;
CREATE TABLE t_nan_tuple_key (t Tuple(Float64, Int32), x Int32) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 4, index_granularity_bytes = 10485760;
INSERT INTO t_nan_tuple_key VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((nan,2),0),((nan,3),0),((nan,4),0),((nan,5),0);

-- The NaNs are visible in the last granule's bounds.
SELECT count() FROM t_nan_tuple_key WHERE t >= (5., 3)
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM (SELECT * FROM t_nan_tuple_key WHERE t >= (5., 3))
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM t_nan_tuple_key WHERE t >= (5., 3) SETTINGS optimize_use_implicit_projections = 0;
SELECT t FROM t_nan_tuple_key WHERE t >= (5., 3) ORDER BY t;

DROP TABLE IF EXISTS t_nan_inside_granule;
CREATE TABLE t_nan_inside_granule (t Tuple(Int32, Float64)) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 4, index_granularity_bytes = 10485760;
INSERT INTO t_nan_inside_granule VALUES ((5,1.0)),((5,nan)),((6,1.0)),((6,2.0)),((7,1.0)),((7,2.0)),((8,1.0)),((8,2.0));

-- (5,nan) sorts between (5,1.0) and (6,1.0), so the granule spanning it has NaN-free bounds.
SELECT count() FROM t_nan_inside_granule WHERE t >= (5, 0.0)
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM t_nan_inside_granule WHERE t >= (5, 0.0) SETTINGS optimize_use_implicit_projections = 0;

-- Controls, none of which may lose exact counting: an Int32 key, a flat Float64 key, an upper-bounded
-- tuple range, and a NaN-hiding key column that no condition reads. Every predicate accepts every row,
-- so exact counting answers it from the index and reads only the single row it produces; losing
-- exactness would read all eight. t_float_key and t_tuple_key_no_nan hold no NaN on purpose, because a
-- NaN there costs exactness legitimately and would make the control vacuous.
DROP TABLE IF EXISTS t_int_key;
CREATE TABLE t_int_key (a Int32, b Int32) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 4, index_granularity_bytes = 10485760;
INSERT INTO t_int_key VALUES (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);
SELECT count() FROM t_int_key WHERE a >= 0
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, log_comment = '05136_int_key';

DROP TABLE IF EXISTS t_float_key;
CREATE TABLE t_float_key (v Float64, x Int32) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 4, index_granularity_bytes = 10485760;
INSERT INTO t_float_key VALUES (1.,0),(2.,0),(10.,1),(500.,1),(600.,0),(700.,0),(800.,0),(900.,0);
SELECT count() FROM t_float_key WHERE v >= 0.
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, log_comment = '05136_float_key';

DROP TABLE IF EXISTS t_tuple_key_no_nan;
CREATE TABLE t_tuple_key_no_nan (t Tuple(Float64, Int32)) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 4, index_granularity_bytes = 10485760;
INSERT INTO t_tuple_key_no_nan VALUES ((1.,1)),((2.,1)),((10.,1)),((500.,7)),((600.,1)),((700.,1)),((800.,1)),((900.,1));
SELECT count() FROM t_tuple_key_no_nan WHERE t <= (1000., 0)
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, log_comment = '05136_tuple_upper_bound';

DROP TABLE IF EXISTS t_unread_nan_key;
CREATE TABLE t_unread_nan_key (i Int32, t Tuple(Float64, Int32)) ENGINE = MergeTree ORDER BY (i, t)
    SETTINGS index_granularity = 4, index_granularity_bytes = 10485760;
INSERT INTO t_unread_nan_key VALUES (0,(nan,2)),(1,(nan,3)),(2,(1.,1)),(3,(2.,1)),(4,(3.,1)),(5,(nan,4)),(6,(nan,5)),(7,(4.,1));
SELECT count() FROM t_unread_nan_key WHERE i >= 0
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, log_comment = '05136_unread_nan_key';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05136_int_key', '05136_float_key', '05136_tuple_upper_bound', '05136_unread_nan_key')
    ORDER BY log_comment;

DROP TABLE t_nan_tuple_key;
DROP TABLE t_nan_inside_granule;
DROP TABLE t_int_key;
DROP TABLE t_float_key;
DROP TABLE t_tuple_key_no_nan;
DROP TABLE t_unread_nan_key;
