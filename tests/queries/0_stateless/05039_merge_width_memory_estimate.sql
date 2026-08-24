-- A merge holds one block from every source part at once, so its fixed cost grows with
-- `source parts * columns`. `merge_memory_estimate_per_source_part_column` turns that into a limit on how
-- many parts one merge may take. An absurdly large estimate makes even two columns exceed the budget, so
-- the merge width falls to its floor of two parts; with the estimate disabled the selector is free to take
-- all of them.
--
-- `merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once` is off so that the width comes
-- from the memory estimate alone and not from how full the partition is.

DROP TABLE IF EXISTS t_merge_width;

CREATE TABLE t_merge_width (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = 1000000000000,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0;

SYSTEM STOP MERGES t_merge_width;
INSERT INTO t_merge_width VALUES (1, 1);
INSERT INTO t_merge_width VALUES (2, 2);
INSERT INTO t_merge_width VALUES (3, 3);
INSERT INTO t_merge_width VALUES (4, 4);
INSERT INTO t_merge_width VALUES (5, 5);
INSERT INTO t_merge_width VALUES (6, 6);
SYSTEM START MERGES t_merge_width;

SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width' AND active;

SET optimize_throw_if_noop = 1;

OPTIMIZE TABLE t_merge_width;
SELECT 'capped', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width' AND active;

ALTER TABLE t_merge_width MODIFY SETTING merge_memory_estimate_per_source_part_column = 0;
OPTIMIZE TABLE t_merge_width;
SELECT 'uncapped', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width' AND active;

SELECT sum(k), sum(v), count() FROM t_merge_width;

DROP TABLE t_merge_width;
