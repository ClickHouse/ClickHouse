-- Regression test for PR #97522: non-deterministic uncompressed_hash in compact
-- MergeTree parts with multiple distinct compression codecs.
--
-- MergeTreeDataPartWriterCompact::addToChecksums iterates a streams_by_codec map
-- when computing each part's uncompressed_hash. Before the fix this was a
-- std::unordered_map with non-deterministic iteration order, so two identical
-- parts could get different uncompressed_hash values. The fix uses std::map.
--
-- Deduplication is disabled here so the two identical inserts stay as two separate
-- compact parts, and we assert their uncompressed_hash values are identical. This
-- targets system.parts.uncompressed_hash_of_compressed_files, which still consumes
-- the checksum path today (unlike insert deduplication, which now hashes the block).

DROP TABLE IF EXISTS t_04548_mc_dedup;

CREATE TABLE t_04548_mc_dedup
(
    id    UInt64   CODEC(LZ4),
    val1  String   CODEC(ZSTD(1)),
    val2  Float64  CODEC(DoubleDelta, LZ4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    non_replicated_deduplication_window = 0,
    min_bytes_for_wide_part = 10485760,
    min_rows_for_wide_part  = 10485760;

SYSTEM STOP MERGES t_04548_mc_dedup;

INSERT INTO t_04548_mc_dedup VALUES (1, 'hello', 1.5), (2, 'world', 2.5), (3, 'foo', 3.5);
-- Identical data, second part: its uncompressed_hash must equal the first's.
INSERT INTO t_04548_mc_dedup VALUES (1, 'hello', 1.5), (2, 'world', 2.5), (3, 'foo', 3.5);

-- Two active parts (deduplication is off).
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_04548_mc_dedup' AND active;

-- Both are compact parts.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_04548_mc_dedup' AND active AND part_type = 'Compact';

-- The two identical compact parts must have the same uncompressed_hash (1 distinct value).
SELECT uniqExact(uncompressed_hash_of_compressed_files) FROM system.parts WHERE database = currentDatabase() AND table = 't_04548_mc_dedup' AND active;

-- Data is intact.
SELECT id, val1, val2 FROM t_04548_mc_dedup ORDER BY id;

DROP TABLE t_04548_mc_dedup;
