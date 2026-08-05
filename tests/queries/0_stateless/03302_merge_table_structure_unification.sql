SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_a;
DROP TABLE IF EXISTS test_b;
DROP TABLE IF EXISTS test_merge;

CREATE TABLE test_a
(
    a UInt8,
    b String,
    c Array(String)
) ENGINE = Memory;

CREATE TABLE test_b
(
    a Int32,
    c Array(Nullable(String)),
    d DateTime('UTC') DEFAULT now(),
) ENGINE = Memory;

INSERT INTO test_a VALUES (1, 'Hello', ['World']);
INSERT INTO test_b VALUES (-1, ['Goodbye'], '2025-01-01 02:03:04');

CREATE TABLE test_merge ENGINE = Merge(currentDatabase(), '^test_');

-- TODO: defaults are not calculated
SELECT * FROM test_merge ORDER BY a;

SELECT '--- table function';
DESCRIBE merge('^test_');

-- Note that this will also pick up the test_merge table, duplicating the results
SELECT * FROM merge('^test_') ORDER BY a;

DROP TABLE test_merge;

SET merge_table_max_tables_to_look_for_schema_inference = 1;

CREATE TABLE test_merge ENGINE = Merge(currentDatabase(), '^test_');

SELECT '--- merge_table_max_tables_to_look_for_schema_inference = 1';
SELECT * FROM test_merge ORDER BY a;

SELECT '--- table function';
DESCRIBE merge('^test_');

SELECT * FROM merge('^test_') ORDER BY a;

DROP TABLE test_merge;

DROP TABLE test_a;
DROP TABLE test_b;
