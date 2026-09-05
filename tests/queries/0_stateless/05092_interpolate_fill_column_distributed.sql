-- A column of `ORDER BY ... WITH FILL` must not also be an `INTERPOLATE` output: the fill rows would
-- overwrite the very column they are ordered by. Only the filling transform checked this, and its
-- sort description carries the written name of the column on some read paths only, so the same query
-- threw over a local table and ran over a `Distributed` one - answering with the fill column replaced
-- by the interpolated expression.

DROP TABLE IF EXISTS t_interpolate_local;
DROP TABLE IF EXISTS t_interpolate_dist;
CREATE TABLE t_interpolate_local (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_interpolate_local SELECT number * 3, number FROM numbers(10);
CREATE TABLE t_interpolate_dist AS t_interpolate_local
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_interpolate_local');

SELECT 'the fill column as an INTERPOLATE output';
SELECT k AS af FROM t_interpolate_local ORDER BY af WITH FILL STEP 2 INTERPOLATE (af AS 42); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT k AS af FROM t_interpolate_dist ORDER BY af WITH FILL STEP 2 INTERPOLATE (af AS 42); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'another column, which is what INTERPOLATE is for';
SELECT k AS af, v FROM t_interpolate_local ORDER BY af WITH FILL STEP 2 INTERPOLATE (v AS 7) LIMIT 5;
SELECT k AS af, v FROM t_interpolate_dist ORDER BY af WITH FILL STEP 2 INTERPOLATE (v AS 7) LIMIT 5;

SELECT 'a column of the same value under a different name is still allowed';
SELECT 1 AS a, 1 AS x ORDER BY a WITH FILL FROM 1 TO 5 INTERPOLATE (x AS x);

DROP TABLE t_interpolate_dist;
DROP TABLE t_interpolate_local;
