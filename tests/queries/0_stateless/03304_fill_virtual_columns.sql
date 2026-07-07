DROP TABLE IF EXISTS test_virtual_columns;

-- { echoOn }
CREATE TABLE test_virtual_columns(a Int32) ENGINE = MergeTree() ORDER BY a;

INSERT INTO test_virtual_columns VALUES (1) (2);

SELECT _part_offset FROM test_virtual_columns;

DELETE FROM test_virtual_columns WHERE a = 1;

SELECT _part_offset FROM test_virtual_columns;

-- { echoOff }
DROP TABLE test_virtual_columns;
