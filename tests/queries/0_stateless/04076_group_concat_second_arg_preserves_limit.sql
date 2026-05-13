-- Regression for #104774: `groupConcat(col, delim)` used to drop the limit parameter.

DROP TABLE IF EXISTS t_group_concat_overload;
CREATE TABLE t_group_concat_overload (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_group_concat_overload SELECT number FROM numbers(5);

SELECT 'baseline:', groupConcat(',', 2)(x) FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'limit kept:', groupConcat(',', 2)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'delim overridden:', groupConcat(',', 3)(x, '|') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'large limit:', groupConcat(',', 100)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

DROP TABLE t_group_concat_overload;
