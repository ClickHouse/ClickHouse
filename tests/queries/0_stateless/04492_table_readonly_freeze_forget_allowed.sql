-- Test that non-mutating partition commands (FREEZE/UNFREEZE) are still allowed on a table
-- marked read-only via the `table_readonly` MergeTree setting.

DROP TABLE IF EXISTS t_readonly_freeze;

CREATE TABLE t_readonly_freeze (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 2;

INSERT INTO t_readonly_freeze SELECT number FROM numbers(8);

ALTER TABLE t_readonly_freeze MODIFY SETTING table_readonly = 1;

-- Mutating partition commands are rejected while read-only.
ALTER TABLE t_readonly_freeze DROP PARTITION 0; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- FREEZE does not modify the table's data, so it is allowed while read-only.
ALTER TABLE t_readonly_freeze FREEZE PARTITION 0 WITH NAME 'freeze_ro';
SELECT 'freeze ok';

-- UNFREEZE of the backup is allowed while read-only.
ALTER TABLE t_readonly_freeze UNFREEZE PARTITION 0 WITH NAME 'freeze_ro';
SELECT 'unfreeze ok';

-- Data is untouched.
SELECT count() FROM t_readonly_freeze;

DROP TABLE t_readonly_freeze;
