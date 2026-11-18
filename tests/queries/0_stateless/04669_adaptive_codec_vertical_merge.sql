-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads mergeTreeCodecBlockCounts and depends on the vertical merge algorithm firing.

-- Adaptive codec selection must apply on vertical merges too. The gathering (non-key) column `n`
-- is written by the per-column vertical-merge stream, which is a different call site than horizontal merges.

DROP TABLE IF EXISTS t_vert_adaptive;

CREATE TABLE t_vert_adaptive (dt DateTime, n UInt64) ENGINE = MergeTree ORDER BY dt
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1,
         enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_rows_to_activate = 1;

INSERT INTO t_vert_adaptive SELECT toDateTime('2020-01-01'), number FROM numbers(100000);
INSERT INTO t_vert_adaptive SELECT toDateTime('2020-01-01'), number FROM numbers(100000, 100000);
OPTIMIZE TABLE t_vert_adaptive FINAL;
SYSTEM FLUSH LOGS part_log;

-- The merge actually used the vertical algorithm; otherwise `n` is written by the horizontal
-- call site and the vertical writer stays untested.
SELECT 'merge_algorithm', merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_vert_adaptive' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- `n` is gathered vertically and is monotonic, so T64 wins on every block.
SELECT 'vertical', max(mapContains(codec_block_counts, 'T64')), max(mapContains(codec_block_counts, 'NONE'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_vert_adaptive) WHERE column = 'n';

SELECT 'roundtrip', count(), sum(n) FROM t_vert_adaptive;

DROP TABLE t_vert_adaptive;
