-- Redundant parentheses must not turn a unique sorting key into an empty effective patch key.
SET enable_lightweight_update = 1, apply_patch_parts = 1, max_threads = 1;
SET log_queries = 1, log_queries_probability = 1, log_profile_events = 1;

DROP TABLE IF EXISTS t_lwu_parenthesized_key;
CREATE TABLE t_lwu_parenthesized_key (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (k)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    patch_parts_version = 'v2', index_granularity = 128, index_granularity_bytes = 1048576;

SYSTEM STOP MERGES t_lwu_parenthesized_key;
INSERT INTO t_lwu_parenthesized_key SELECT number, number FROM numbers(20000);
UPDATE t_lwu_parenthesized_key SET v = v + 1 WHERE 1;

SELECT sum(v) FROM t_lwu_parenthesized_key WHERE k = 0
SETTINGS log_comment = 'lwu_parenthesized_key_pruning';

SYSTEM FLUSH LOGS query_log;
SELECT count() = 1, max(ProfileEvents['PatchesReadRows']) BETWEEN 1 AND 1024
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment = 'lwu_parenthesized_key_pruning';

-- The same existing patch remains usable after changing only the key's spelling.
ALTER TABLE t_lwu_parenthesized_key MODIFY ORDER BY k;
SELECT sum(v) FROM t_lwu_parenthesized_key;
SYSTEM START MERGES t_lwu_parenthesized_key;
OPTIMIZE TABLE t_lwu_parenthesized_key FINAL;
SELECT sum(v) FROM t_lwu_parenthesized_key SETTINGS apply_patch_parts = 0;
DROP TABLE t_lwu_parenthesized_key;
