-- A merge that clears expired columns is chosen by `TTLColumnDeleteMergeSelector`, and it holds one block
-- from every source part at once like any other merge. `merge_memory_estimate_per_source_part_column` has
-- to narrow it too: otherwise a wide table on a small server keeps selecting the same too-wide column TTL
-- merge, which fails with `MEMORY_LIMIT_EXCEEDED` every time. An absurdly large estimate makes even three
-- columns exceed the budget, so the merge width falls to its floor of two parts.
--
-- The table has a column TTL and no rows TTL, so the column TTL selector is the only one with work to do.
-- Every column TTL is already due, and `merge_with_ttl_timeout = 0` lets TTL merges run back to back.

DROP TABLE IF EXISTS t_merge_width_column_ttl;

CREATE TABLE t_merge_width_column_ttl (k UInt64, d DateTime, v UInt64 TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = 1000000000000,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_merge_width_column_ttl;
INSERT INTO t_merge_width_column_ttl VALUES (1, '2000-01-01 00:00:00', 1);
INSERT INTO t_merge_width_column_ttl VALUES (2, '2000-01-01 00:00:00', 2);
INSERT INTO t_merge_width_column_ttl VALUES (3, '2000-01-01 00:00:00', 3);
INSERT INTO t_merge_width_column_ttl VALUES (4, '2000-01-01 00:00:00', 4);
INSERT INTO t_merge_width_column_ttl VALUES (5, '2000-01-01 00:00:00', 5);
INSERT INTO t_merge_width_column_ttl VALUES (6, '2000-01-01 00:00:00', 6);
SYSTEM START MERGES t_merge_width_column_ttl;

SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_column_ttl' AND active;

SET optimize_throw_if_noop = 1;

OPTIMIZE TABLE t_merge_width_column_ttl;
SELECT 'capped', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_column_ttl' AND active;

SYSTEM FLUSH LOGS part_log;

SELECT merge_reason, length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_column_ttl' AND event_type = 'MergeParts';

DROP TABLE t_merge_width_column_ttl;
