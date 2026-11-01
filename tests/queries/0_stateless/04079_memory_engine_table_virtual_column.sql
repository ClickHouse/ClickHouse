-- Test that the _table virtual column works natively in Memory engine

DROP TABLE IF EXISTS test_memory_virt;
DROP TABLE IF EXISTS test_memory_virt_empty;

CREATE TABLE test_memory_virt (x UInt64, s String) ENGINE = Memory;
INSERT INTO test_memory_virt VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TABLE test_memory_virt_empty (x UInt64) ENGINE = Memory;

-- { echoOn }
SELECT _table FROM test_memory_virt LIMIT 1;
SELECT x, _table FROM test_memory_virt ORDER BY x;
SELECT DISTINCT _table FROM test_memory_virt;
SELECT x FROM test_memory_virt WHERE _table = 'test_memory_virt' ORDER BY x;
SELECT count() FROM test_memory_virt WHERE _table = 'nonexistent';
SELECT count() FROM test_memory_virt_empty;
SELECT _table FROM test_memory_virt_empty;
-- { echoOff }

DROP TABLE test_memory_virt;
DROP TABLE test_memory_virt_empty;
