-- Regression for #104774: the two-argument form `groupConcat(col, delim)`
-- went through a code path that rebuilt the function node from scratch
-- and silently dropped any pre-existing parameters, including the optional
-- limit, so `groupConcat(',', N)(col, '/')` returned every row.

DROP TABLE IF EXISTS t_group_concat_overload;
CREATE TABLE t_group_concat_overload (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_group_concat_overload SELECT number FROM numbers(5);

-- Reference: parameter limit alone works.
SELECT 'baseline:', groupConcat(',', 2)(x) FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

-- Bug: limit was silently dropped, returning all 5 elements.
SELECT 'limit kept:', groupConcat(',', 2)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

-- The two-argument delimiter overrides the parameter delimiter.
SELECT 'delim overridden:', groupConcat(',', 3)(x, '|') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

-- Limit large enough that all rows fit — output unchanged from the no-limit form.
SELECT 'large limit:', groupConcat(',', 100)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

DROP TABLE t_group_concat_overload;
