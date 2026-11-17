SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table_join_1;
CREATE TABLE test_table_join_1
(
    id UInt8,
    value String
)
ENGINE = TinyLog;

INSERT INTO test_table_join_1 VALUES (0, 'Value_0');

DROP TABLE IF EXISTS test_table_join_2;
CREATE TABLE test_table_join_2
(
    id UInt16,
    value String
)
ENGINE = TinyLog;

INSERT INTO test_table_join_2 VALUES (0, 'Value_1');

SELECT
    toTypeName(t2_value),
    t2.value AS t2_value
FROM test_table_join_1 AS t1
INNER JOIN test_table_join_2 USING (id); -- { serverError UNKNOWN_IDENTIFIER };

SELECT
    toTypeName(t2_value),
    t2.value AS t2_value
FROM test_table_join_1 AS t1
INNER JOIN test_table_join_2 AS t2 USING (id);

DROP TABLE test_table_join_1;
DROP TABLE test_table_join_2;
