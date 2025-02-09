SET enable_analyzer = 1;

SELECT arrayJoin([1, 2, 3]);

SELECT '--';

SELECT arrayJoin([1, 2, 3]) AS a, arrayJoin([1, 2, 3]);

SELECT '--';

SELECT arrayJoin([1, 2, 3]) AS a, a;

SELECT '--';

SELECT arrayJoin([[1, 2, 3]]) AS a, arrayJoin(a) AS b;

SELECT '--';

SELECT arrayJoin([1, 2, 3]) AS a, arrayJoin([1, 2, 3, 4]) AS b;

SELECT '--';

SELECT arrayMap(x -> arrayJoin([1, 2, 3]), [1, 2, 3]);

SELECT arrayMap(x -> arrayJoin(x), [[1, 2, 3]]); -- { serverError BAD_ARGUMENTS }

SELECT arrayMap(x -> arrayJoin(cast(x, 'Array(UInt8)')), [[1, 2, 3]]); -- { serverError BAD_ARGUMENTS }

SELECT '--';

SELECT arrayMap(x -> x + a, [1, 2, 3]), arrayJoin([1,2,3]) as a;

SELECT '--';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value_1 Array(UInt8),
    value_2 Array(UInt8),
) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO test_table VALUES (0, [1, 2, 3], [1, 2, 3, 4]);

SELECT id, arrayJoin(value_1) FROM test_table;

SELECT '--';

SELECT id, arrayJoin(value_1) AS a, a FROM test_table;

-- SELECT '--';

-- SELECT id, arrayJoin(value_1), arrayJoin(value_2) FROM test_table;

-- SELECT '--';

-- SELECT id, arrayJoin(value_1), arrayJoin(value_2), arrayJoin([5, 6]) FROM test_table;

DROP TABLE test_table;
