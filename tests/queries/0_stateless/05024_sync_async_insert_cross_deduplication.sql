-- Deduplication spans synchronous and asynchronous inserts: both write the same
-- unified hash, so a retry sent in one mode is a duplicate of an attempt sent in
-- the other. See docs/concepts/features/operations/insert/deduplicating-inserts-on-retries.mdx

CREATE TABLE test
    (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY id;

SET deduplicate_insert = 'enable';
SET async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_min_ms = 10000, async_insert_busy_timeout_max_ms = 50000;

-- Synchronous insert first, then the same data asynchronously.

TRUNCATE TABLE test;

SET async_insert = 0;
INSERT INTO test VALUES (1, 'one line');

SET async_insert = 1, wait_for_async_insert = 0;
INSERT INTO test VALUES (1, 'one line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;

SELECT 'case: sync insert then async insert of the same data', * FROM test ORDER BY id;

-- Asynchronous insert first, then the same data synchronously.

TRUNCATE TABLE test;

SET async_insert = 1, wait_for_async_insert = 0;
INSERT INTO test VALUES (2, 'other line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;

SET async_insert = 0;
INSERT INTO test VALUES (2, 'other line');

SELECT 'case: async insert then sync insert of the same data', * FROM test ORDER BY id;

-- The same crossing with a user token instead of a data hash. The token, and not
-- the data, identifies the insert, so the async attempt is dropped even though it
-- carries different values.

TRUNCATE TABLE test;

SET async_insert = 0;
INSERT INTO test SETTINGS insert_deduplication_token = 'shared_token' VALUES (3, 'sync value');

SET async_insert = 1, wait_for_async_insert = 0;
INSERT INTO test SETTINGS insert_deduplication_token = 'shared_token' VALUES (4, 'async value');
SYSTEM FLUSH ASYNC INSERT QUEUE test;

SELECT 'case: sync then async with the same insert_deduplication_token', * FROM test ORDER BY id;

-- With deduplication off the crossing must not deduplicate.

TRUNCATE TABLE test;

SET deduplicate_insert = 'disable';

SET async_insert = 0;
INSERT INTO test VALUES (5, 'one line');

SET async_insert = 1, wait_for_async_insert = 0;
INSERT INTO test VALUES (5, 'one line');
SYSTEM FLUSH ASYNC INSERT QUEUE test;

SELECT 'case: sync then async, deduplicate_insert=\'disable\'', * FROM test ORDER BY id;

DROP TABLE test SYNC;
