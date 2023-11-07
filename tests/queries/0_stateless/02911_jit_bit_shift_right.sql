SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    pkey UInt32,
    c5 UInt32,
    c7 UInt32,
    PRIMARY KEY(pkey)
) ENGINE = MergeTree;

INSERT INTO test_table VALUES (40000, 73, 1);
INSERT INTO test_table VALUES (52000, 85, 1);
INSERT INTO test_table VALUES (53000, 1, 8);
INSERT INTO test_table VALUES (59000, 1, 72);
INSERT INTO test_table VALUES (62000, 16, 17);

SET compile_expressions = 0;

SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);

SET compile_expressions = 1;

SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);
SELECT count(*) FROM test_table WHERE c7 <= bitShiftRight(c5, pkey);

DROP TABLE test_table;
