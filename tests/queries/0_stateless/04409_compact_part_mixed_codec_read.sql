-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: we want specific MergeTree behaviour.

-- Compact parts share one data.bin, so different-codec columns (ZSTD, NONE) make adjacent blocks with different methods, which the reader must tolerate.

DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (a UInt64 CODEC(ZSTD), b UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact SELECT number, number FROM numbers(100000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
SELECT count(), sum(a), sum(b) FROM t_compact;

DROP TABLE t_compact;
