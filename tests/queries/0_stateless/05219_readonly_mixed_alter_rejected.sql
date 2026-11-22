-- Tags: no-replicated-database, no-shared-merge-tree
-- `table_readonly` is a plain MergeTree setting.

-- The `table_readonly` 1 -> 0 toggle restarts the background workers of a table that was attached
-- read-only. That happens only in the settings-only ALTER branch, so a settings change mixed with
-- other commands in one ALTER must be rejected for a read-only table: otherwise the table would
-- become durably writable with its workers absent until a restart.

DROP TABLE IF EXISTS readonly_mixed_alter SYNC;
CREATE TABLE readonly_mixed_alter (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO readonly_mixed_alter SELECT number FROM numbers(10);
ALTER TABLE readonly_mixed_alter MODIFY SETTING table_readonly = 1;
DETACH TABLE readonly_mixed_alter;
ATTACH TABLE readonly_mixed_alter;

-- A comma after `MODIFY SETTING` continues the settings list, so the settings command goes last.
ALTER TABLE readonly_mixed_alter MODIFY COMMENT 'writable', MODIFY SETTING table_readonly = 0; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ALTER TABLE readonly_mixed_alter ADD COLUMN y UInt8, MODIFY SETTING table_readonly = 0; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ALTER TABLE readonly_mixed_alter MODIFY COMMENT 'writable', RESET SETTING table_readonly; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'readonly_mixed_alter';
INSERT INTO readonly_mixed_alter VALUES (100); -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- The settings-only toggle works and restarts the workers: a mutation can execute.
ALTER TABLE readonly_mixed_alter MODIFY SETTING table_readonly = 0;
ALTER TABLE readonly_mixed_alter DELETE WHERE x = 0 SETTINGS mutations_sync = 1;
SELECT count() FROM readonly_mixed_alter;
ALTER TABLE readonly_mixed_alter MODIFY COMMENT 'writable';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'readonly_mixed_alter';

DROP TABLE readonly_mixed_alter SYNC;
