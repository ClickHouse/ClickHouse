DROP TABLE IF EXISTS test;

CREATE TABLE test(
    id UInt64,
    numbers Array(Int64)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test VALUES(0, [-2, -1, 0, 1]);
INSERT INTO test VALUES(1, [1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 6, 7]);
INSERT INTO test VALUES (2, [1, 2, 3, 4, 5, 6, 7, 8]);
INSERT INTO test VALUES(3, [1, 3, 7, 10]);
INSERT INTO test VALUES(4, [0, 0, 0]);
INSERT INTO test VALUES(5, [10, 10, 10]);

SELECT indexOfAssumeSorted(numbers, 4) FROM test WHERE id = 1;
SELECT indexOfAssumeSorted(numbers, 5) FROM test WHERE id = 2;
SELECT indexOfAssumeSorted(numbers, 5) FROM test WHERE id = 3;
SELECT indexOfAssumeSorted(numbers, 1) FROM test WHERE id = 4;
SELECT indexOfAssumeSorted(numbers, 1) FROM test WHERE id = 5;

SELECT indexOfAssumeSorted([1, 2, 2, 2, 3, 3, 3, 4, 4], 4);
SELECT indexOfAssumeSorted([10, 10, 10], 1);
SELECT indexOfAssumeSorted([1, 1, 1], 10);

SELECT indexOfAssumeSorted(numbers, toUInt64(if(id = 1, 3, id))) FROM test ORDER BY id;

SELECT indexOfAssumeSorted([1, 3, 5, 7, 9], number) FROM numbers(11);

-- ColumnConst(Array) with mixed signed/unsigned needles and first-match duplicates.
SELECT indexOfAssumeSorted(CAST([-2, -1, 1, 1, 1, 3] AS Array(Int64)), number) FROM numbers(5);
SELECT indexOfAssumeSorted(CAST([0, 0, 2, 2, 5] AS Array(UInt64)), toInt64(number) - 1) FROM numbers(7);

-- Enum values are numerically sorted, but their names may not be sorted.
WITH CAST(['z', 'a'], 'Array(Enum8(\'z\' = 1, \'a\' = 2))') AS e
SELECT indexOfAssumeSorted(e, if(number = 0, 'z', 'a')) FROM numbers(2);

-- String arrays use lexicographic ordering, while an Enum needle compares its numeric Field.
SELECT
    indexOfAssumeSorted(
        CAST(['10', '2'] AS Array(String)),
        CAST(if(number = 0, '2', '10') AS Enum8('2' = 2, '10' = 10)))
FROM numbers(2);

DROP TABLE IF EXISTS test;
