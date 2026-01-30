CREATE TABLE test
    (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY id;


SET async_insert = 0;

TRUNCATE TABLE test;
SET deduplicate_insert = 'backward_compatible_choice';
SET insert_deduplicate = 1;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SELECT 'case: sync insert, insert_deduplicate=1 is main setting', * FROM test ORDER BY id;

TRUNCATE TABLE test;
SET deduplicate_insert = 'backward_compatible_choice';
SET insert_deduplicate = 0;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SELECT 'case: sync insert, insert_deduplicate=0 is main setting', * FROM test ORDER BY id;

TRUNCATE TABLE test;
SET deduplicate_insert = 'enable';
SET insert_deduplicate = 0;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SELECT 'case: sync insert, deduplicate_insert=\'ENABLE\' is main setting', * FROM test ORDER BY id;

TRUNCATE TABLE test;
SET deduplicate_insert = 'disable';
SET insert_deduplicate = 1;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SELECT 'case: sync insert, deduplicate_insert=\'DISABLE\' is main setting', * FROM test ORDER BY id;


SET async_insert = 1, wait_for_async_insert = 0;
SET async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_min_ms=10000, async_insert_busy_timeout_max_ms=50000;

TRUNCATE TABLE test;
SET deduplicate_insert = 'backward_compatible_choice';
SET async_insert_deduplicate = 1;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;
SELECT 'case: async insert, async_insert_deduplicate=1 is main setting', * FROM test ORDER BY id;

TRUNCATE TABLE test;
SET deduplicate_insert = 'backward_compatible_choice';
SET async_insert_deduplicate = 0;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;
SELECT 'case: async insert, async_insert_deduplicate=0 is main setting', * FROM test ORDER BY id;

TRUNCATE TABLE test;
SET deduplicate_insert = 'enable';
SET async_insert_deduplicate = 0;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;
SELECT 'case: async insert, deduplicate_insert=\'ENABLE\' is main setting', * FROM test ORDER BY id;

TRUNCATE TABLE test;
SET deduplicate_insert = 'disable';
SET async_insert_deduplicate = 1;
INSERT INTO test VALUES (1, 'one line');
INSERT INTO test VALUES (1, 'one line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;
SELECT 'case: async insert, deduplicate_insert=\'DISABLE\' is main setting', * FROM test ORDER BY id;

DROP TABLE test SYNC;
