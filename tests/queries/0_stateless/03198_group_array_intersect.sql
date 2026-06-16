DROP TABLE IF EXISTS test_numbers__fuzz_29;
SET max_threads=1, max_insert_threads=1;
CREATE TABLE test_numbers__fuzz_29 (`a` Array(Nullable(FixedString(19)))) ENGINE = MergeTree ORDER BY a SETTINGS allow_nullable_key=1;

INSERT INTO test_numbers__fuzz_29 VALUES ([1,2,3,4,5,6]);
INSERT INTO test_numbers__fuzz_29 VALUES ([1,2,4,5]);
INSERT INTO test_numbers__fuzz_29 VALUES ([1,4,3,0,5,5,5]);

SELECT arraySort(groupArrayIntersect(*)) FROM test_numbers__fuzz_29 GROUP BY a WITH ROLLUP ORDER BY ALL;

DROP TABLE test_numbers__fuzz_29;
