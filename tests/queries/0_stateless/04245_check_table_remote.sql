-- Tags: no-s3-storage
-- Test CHECK TABLE ... REMOTE syntax parsing and basic behavior on local disk.
-- Parts on local disk should pass with "Skipped: not on remote disk." message.

SET check_query_single_value_result = 0;

DROP TABLE IF EXISTS t_check_remote;

CREATE TABLE t_check_remote (key UInt64, value String)
ENGINE = MergeTree()
ORDER BY key;

-- Empty table: no parts to check
CHECK TABLE t_check_remote REMOTE SETTINGS max_threads = 1;

INSERT INTO t_check_remote VALUES (1, 'a'), (2, 'b');
INSERT INTO t_check_remote VALUES (3, 'c'), (4, 'd');

SELECT '-- two parts, local disk, REMOTE should skip all';
CHECK TABLE t_check_remote REMOTE SETTINGS max_threads = 1;

SELECT '-- REMOTE with PARTITION';
CHECK TABLE t_check_remote PARTITION tuple() REMOTE SETTINGS max_threads = 1;

-- Verify that normal CHECK TABLE still works alongside REMOTE
SELECT '-- normal CHECK TABLE still works';
CHECK TABLE t_check_remote SETTINGS max_threads = 1;

-- single_value_result mode: should return 1 (all passed/skipped)
SET check_query_single_value_result = 1;
SELECT '-- single value result with REMOTE';
CHECK TABLE t_check_remote REMOTE;

DROP TABLE t_check_remote;
