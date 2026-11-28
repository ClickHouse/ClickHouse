-- the planner turns arrayJoin(emptyArrayToSingle(x)) into LEFT ARRAY JOIN x, with the same rows as the function

SET query_plan_lower_array_join_function = 1;

DROP TABLE IF EXISTS t_left_aj;

CREATE TABLE t_left_aj (id UInt64, arr Array(String), narr Array(Nullable(UInt8)), lc Array(LowCardinality(String)))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left_aj VALUES (1, ['a', 'b'], [1, NULL], ['x']), (2, [], [], []), (3, ['c'], [3], ['y', 'z']);

SELECT '-- plan';
SELECT count() FROM (EXPLAIN SELECT id, arrayJoin(emptyArrayToSingle(arr)) FROM t_left_aj) WHERE explain LIKE '%LEFT ARRAY JOIN%';

SELECT '-- rows';
SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS a FROM t_left_aj ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(narr)) AS a FROM t_left_aj ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(lc)) AS a, toTypeName(a) FROM t_left_aj ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS a FROM t_left_aj WHERE a != 'b' ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS a, arrayJoin(emptyArrayToSingle(narr)) AS b FROM t_left_aj ORDER BY id, a, b;
SELECT id, emptyArrayToSingle(arr) AS e, arrayJoin(e) AS a FROM t_left_aj ORDER BY id, a;
SELECT arrayJoin(emptyArrayToSingle(CAST([], 'Array(UInt8)')));

SELECT '-- the same rows from the function';
SET query_plan_lower_array_join_function = 0;
SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS a FROM t_left_aj ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(narr)) AS a FROM t_left_aj ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(lc)) AS a, toTypeName(a) FROM t_left_aj ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS a FROM t_left_aj WHERE a != 'b' ORDER BY id, a;
SELECT id, arrayJoin(emptyArrayToSingle(arr)) AS a, arrayJoin(emptyArrayToSingle(narr)) AS b FROM t_left_aj ORDER BY id, a, b;
SELECT id, emptyArrayToSingle(arr) AS e, arrayJoin(e) AS a FROM t_left_aj ORDER BY id, a;
SELECT arrayJoin(emptyArrayToSingle(CAST([], 'Array(UInt8)')));

DROP TABLE t_left_aj;
