-- Tags: no-fasttest
-- Regression test for PR #97522: non-deterministic `uncompressed_hash` in
-- compact `MergeTree` parts with multiple distinct compression codecs.
--
-- `MergeTreeWriterCompact` groups substreams by codec in a `streams_by_codec`
-- map. Before the fix, this was `std::unordered_map`, whose iteration order is
-- non-deterministic, so the same data could produce different `uncompressed_hash`
-- values across inserts, breaking deduplication. The fix uses `std::map`.
--
-- This test uses columns with three distinct codecs (LZ4, ZSTD, DoubleDelta+LZ4)
-- to ensure multiple entries in `streams_by_codec`. Forcing compact parts via
-- `min_bytes_for_wide_part = 10485760`. Two identical inserts must be deduplicated
-- to exactly one active part.

DROP TABLE IF EXISTS t_04042_mc_dedup;

CREATE TABLE t_04042_mc_dedup
(
    id    UInt64   CODEC(LZ4),
    val1  String   CODEC(ZSTD(1)),
    val2  Float64  CODEC(DoubleDelta, LZ4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    non_replicated_deduplication_window = 1000,
    min_bytes_for_wide_part = 10485760,
    min_rows_for_wide_part  = 10485760;

SYSTEM STOP MERGES t_04042_mc_dedup;

INSERT INTO t_04042_mc_dedup VALUES (1, 'hello', 1.5), (2, 'world', 2.5), (3, 'foo', 3.5);
-- Identical insert: must be deduplicated (uncompressed_hash must be deterministic)
INSERT INTO t_04042_mc_dedup VALUES (1, 'hello', 1.5), (2, 'world', 2.5), (3, 'foo', 3.5);

-- Exactly one active part after two identical inserts
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_04042_mc_dedup' AND active;

-- Data is intact
SELECT id, val1, val2 FROM t_04042_mc_dedup ORDER BY id;

DROP TABLE t_04042_mc_dedup;
