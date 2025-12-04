DROP TABLE IF EXISTS source_table, target_table, target_table_remote_sync, target_table_remote_async, async_insert_mv, sync_insert_mv;
DROP USER IF EXISTS 03748_test_user_sync, 03748_test_user_async;

CREATE USER 03748_test_user_sync HOST ANY IDENTIFIED WITH NO_PASSWORD SETTINGS async_insert = 0;
CREATE USER 03748_test_user_async HOST ANY IDENTIFIED WITH NO_PASSWORD SETTINGS async_insert = 1;
GRANT ALL ON target_table TO 03748_test_user_sync;
GRANT ALL ON target_table TO 03748_test_user_async;

CREATE TABLE source_table (
    id UInt64,
    data String
)
ENGINE=MergeTree()
ORDER BY id;

CREATE TABLE target_table (
    id UInt64,
    data String
)
ENGINE=MergeTree()
ORDER BY id;

CREATE TABLE target_table_remote_sync (
    id UInt64,
    data String
)
AS remote('127.0.0.2', currentDatabase(), 'target_table', '03748_test_user_sync');

CREATE TABLE target_table_remote_async (
    id UInt64,
    data String
)
AS remote('127.0.0.2', currentDatabase(), 'target_table', '03748_test_user_async');

CREATE MATERIALIZED VIEW async_insert_mv TO target_table_remote_async AS
SELECT * FROM source_table;

CREATE MATERIALIZED VIEW sync_insert_mv TO target_table_remote_sync AS
SELECT * FROM source_table;

-- Default setting, unset async_insert, so its default is 0
SET async_insert = DEFAULT;
-- One type of inserts each
INSERT INTO source_table (id, data) VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');
SET async_insert = 1;
-- One type of inserts each
INSERT INTO source_table (id, data) VALUES (4, 'test4'), (5, 'test5'), (6, 'test6');
SET async_insert = 0;
-- This time both inserts have async_insert = 0, so 2 async and 4 sync inserts in total
INSERT INTO source_table (id, data) VALUES (7, 'test7'), (8, 'test8'), (9, 'test9');

SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log
WHERE query_kind = 'Insert' AND type = 'QueryFinish'
  AND user IN ('03748_test_user_sync', '03748_test_user_async')
--  AND current_database = currentDatabase() -- to silent strange style check warning
  AND tables = [currentDatabase() || '.target_table']
GROUP BY Settings['async_insert']
ORDER BY count() ASC;

SELECT * FROM target_table ORDER BY id;
