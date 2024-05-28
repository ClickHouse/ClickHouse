SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_table_ordinary;
CREATE TABLE test_table_ordinary
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table_ordinary VALUES (0, 'Value_0');
INSERT INTO test_table_ordinary VALUES (0, 'Value_0');
INSERT INTO test_table_ordinary VALUES (1, 'Value_1');
INSERT INTO test_table_ordinary VALUES (2, 'Value_2');

DROP TABLE IF EXISTS test_table_replacing;
CREATE TABLE test_table_replacing
(
    id UInt64,
    value String
) ENGINE=ReplacingMergeTree ORDER BY id;

SYSTEM STOP MERGES test_table_replacing;

INSERT INTO test_table_replacing VALUES (0, 'Value_0');
INSERT INTO test_table_replacing VALUES (0, 'Value_0');
INSERT INTO test_table_replacing VALUES (1, 'Value_1');
INSERT INTO test_table_replacing VALUES (2, 'Value_2');

-- { echoOn }

SELECT * FROM (SELECT * FROM test_table_ordinary ORDER BY id) FINAL;

SELECT '--';

SELECT * FROM (SELECT * FROM (SELECT * FROM test_table_ordinary ORDER BY id)) FINAL;

SELECT '--';

SELECT * FROM (SELECT * FROM test_table_ordinary WHERE id = 0) FINAL;

SELECT '--';

SELECT * FROM (SELECT * FROM (SELECT * FROM test_table_ordinary WHERE id = 0)) FINAL;

SELECT '--';

SELECT * FROM test_table_replacing FINAL ORDER BY id;

SELECT '--';

SELECT * FROM (SELECT * FROM test_table_replacing ORDER BY id) FINAL;

SELECT '--';

SELECT * FROM (SELECT * FROM (SELECT * FROM test_table_replacing ORDER BY id)) FINAL;

SELECT '--';

SELECT * FROM test_table_replacing FINAL WHERE id = 0;

SELECT '--';

SELECT * FROM (SELECT * FROM test_table_replacing WHERE id = 0) FINAL;

SELECT '--';

SELECT * FROM (SELECT * FROM (SELECT * FROM test_table_replacing WHERE id = 0)) FINAL;

-- { echoOff }

DROP TABLE test_table_ordinary;
DROP TABLE test_table_replacing;
