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

DROP TABLE IF EXISTS float_test;

CREATE TABLE float_test(
    id UInt64,
    numbers Array(Float64),
    needle Float64
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO float_test VALUES
    (0, [1, 2, nan, nan, nan], 2),
    (1, [1, 2, nan, nan, nan], 1),
    (2, [1, 2, nan, nan, nan], nan);

SELECT indexOfAssumeSorted(numbers, needle) FROM float_test ORDER BY id;

DROP TABLE IF EXISTS float_test;

DROP TABLE IF EXISTS test;
