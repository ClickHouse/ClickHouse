-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: compact part formation must be deterministic.

-- Mixed-codec compact stream (ZSTD, NONE) read through the uncompressed cache path.

DROP TABLE IF EXISTS t_compact_uc;
CREATE TABLE t_compact_uc (a UInt64 CODEC(ZSTD), b UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact_uc SELECT number, number FROM numbers(100000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_uc' AND active;
SELECT count(), sum(a), sum(b) FROM t_compact_uc SETTINGS use_uncompressed_cache = 1;
SELECT count(), sum(a), sum(b) FROM t_compact_uc SETTINGS use_uncompressed_cache = 1;

DROP TABLE t_compact_uc;
