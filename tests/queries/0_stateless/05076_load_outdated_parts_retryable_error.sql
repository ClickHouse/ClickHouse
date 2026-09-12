-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: SharedMergeTree doesn't load inactive parts to memory after restart

DROP TABLE IF EXISTS t_load_outdated_parts;

-- Outdated parts must survive DETACH/ATTACH to be loaded in the background after ATTACH.
CREATE TABLE t_load_outdated_parts (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS old_parts_lifetime = 600;

INSERT INTO t_load_outdated_parts VALUES (1);
INSERT INTO t_load_outdated_parts VALUES (2);
INSERT INTO t_load_outdated_parts VALUES (3);
OPTIMIZE TABLE t_load_outdated_parts FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts' AND NOT active;

DETACH TABLE t_load_outdated_parts;

-- Every attempt to load an outdated part fails with a retryable error.
-- The server must not terminate, the loading must be retried later.
SYSTEM ENABLE FAILPOINT merge_tree_load_outdated_parts_retryable_error;
ATTACH TABLE t_load_outdated_parts;

SELECT count() FROM t_load_outdated_parts;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts' AND NOT active;

SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_retryable_error;
SYSTEM WAIT LOADING PARTS t_load_outdated_parts;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts' AND NOT active;

DROP TABLE t_load_outdated_parts;
