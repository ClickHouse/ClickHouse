-- Each skip-index substream's packed/per-file layout is decided per part at write time, based
-- only on the substream's serialized size relative to packed_skip_index_max_bytes. This test
-- walks a single index through both directions:
--
--   1. Small initial part      -> bloom_filter fits   -> packed (skp_idx.packed exists)
--   2. Two small parts merged  -> combined still fits -> packed
--   3. After many INSERTs and OPTIMIZE FINAL, the merged part's bloom_filter grows past the
--      threshold and spills to a standalone per-file substream (skp_idx_bf_w.idx + .cmrk2)
--   4. ALTER DELETE rewrites the part keeping only a small slice, shrinking the bloom_filter
--      back under the threshold -> packed again.
--
-- secondary_indices_compressed_bytes stays > 0 in both layouts, so it can't prove the spill on
-- its own. Instead we track the layout via system.parts.files (a file count, object-storage
-- safe): a packed bloom_filter lives inside skp_idx.packed (one shared archive file), while a
-- spilled one becomes two standalone files (.idx + .cmrk2), so the part's file count goes up by
-- one on the spill and back down on the repack. We record the count per phase in a side table
-- and assert the relative change (never absolute counts, which random merge-tree settings
-- perturb). EXPLAIN indexes = 1 additionally confirms the index keeps gating granules throughout.

DROP TABLE IF EXISTS t_pack_trans;
DROP TABLE IF EXISTS t_pack_trans_files;
CREATE TABLE t_pack_trans
(
    id UInt64,
    w String,
    INDEX bf_w w TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = 4096,
         auto_statistics_types = '',
         index_granularity = 1024,
         -- Pinned: a randomized low ratio sparse-serializes some columns per part, adding column
         -- files that break this test's "phases differ only in index layout" file-count assumption.
         ratio_of_defaults_for_sparse_serialization = 1.0;

-- Remembers the active part's file count at each phase so later phases can compare against the
-- packed baseline without printing absolute counts.
CREATE TABLE t_pack_trans_files (phase String, files UInt64) ENGINE = Memory;

-- ---------------------------------------------------------------------------------------------
-- 1. One small part: bloom_filter is well under the threshold -> packed.
-- ---------------------------------------------------------------------------------------------
INSERT INTO t_pack_trans SELECT number, toString(number * 7919) FROM numbers(1024);
SELECT 'phase1_small_part_index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
SELECT 'phase1_filtered_lookup', count() FROM t_pack_trans WHERE w = '7919';
SELECT 'phase1_index_in_plan',
       countIf(explain LIKE '%Name: bf_w%')
FROM (EXPLAIN indexes = 1 SELECT * FROM t_pack_trans WHERE w = '7919') AS s
SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_pack_trans SETTINGS check_query_single_value_result = 1;

-- ---------------------------------------------------------------------------------------------
-- 2. Two more small INSERTs + merge: each new part still small enough to pack, and the merged
--    part stays under the threshold too.
-- ---------------------------------------------------------------------------------------------
INSERT INTO t_pack_trans SELECT number + 1024, toString(number * 31337) FROM numbers(1024);
INSERT INTO t_pack_trans SELECT number + 2048, toString(number * 53)    FROM numbers(1024);
OPTIMIZE TABLE t_pack_trans FINAL;
-- Packed baseline: a single merged part with the index inside the archive. Phase 3 (also a
-- merged part, so the same per-column bookkeeping) is compared against this count.
INSERT INTO t_pack_trans_files SELECT 'phase2_packed', files
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
SELECT 'phase2_index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
SELECT 'phase2_filtered_lookup_old', count() FROM t_pack_trans WHERE w = '7919';
SELECT 'phase2_filtered_lookup_new', count() FROM t_pack_trans WHERE w = '31337';
CHECK TABLE t_pack_trans SETTINGS check_query_single_value_result = 1;

-- ---------------------------------------------------------------------------------------------
-- 3. Many more INSERTs and a merge: the merged part's bloom_filter overshoots the threshold
--    and spills to a standalone per-file substream. The index must still gate granules.
-- ---------------------------------------------------------------------------------------------
INSERT INTO t_pack_trans SELECT number + 3072, toString(number * 991) FROM numbers(10240);
OPTIMIZE TABLE t_pack_trans FINAL;
INSERT INTO t_pack_trans_files SELECT 'phase3_spilled', files
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
SELECT 'phase3_index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
-- The spill is the actual signal under test: the standalone .idx + .cmrk2 files replace the
-- single shared archive file, so this merged part has strictly more files than the packed
-- merged part of phase 2 (same column layout, only the index layout differs).
SELECT 'phase3_spilled_has_more_files_than_packed',
       (SELECT files FROM t_pack_trans_files WHERE phase = 'phase3_spilled')
     > (SELECT files FROM t_pack_trans_files WHERE phase = 'phase2_packed');
SELECT 'phase3_filtered_lookup', count() FROM t_pack_trans WHERE w = '7919';
CHECK TABLE t_pack_trans SETTINGS check_query_single_value_result = 1;

-- ---------------------------------------------------------------------------------------------
-- 4. ALTER DELETE the large slice: the rewritten part has only a fraction of the rows and the
--    bloom_filter shrinks back under the threshold -> packed again.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE t_pack_trans DELETE WHERE id >= 1024 SETTINGS mutations_sync = 2;
INSERT INTO t_pack_trans_files SELECT 'phase4_repacked', files
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
SELECT 'phase4_index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
-- Repacked: the bloom_filter is back under the threshold, so the standalone .idx + .cmrk2 files
-- collapse into the shared archive and the part has fewer files than the spilled phase 3 (this
-- mutated part shares phase 3's column layout, so only the index layout moves the count).
SELECT 'phase4_repacked_fewer_files_than_spilled',
       (SELECT files FROM t_pack_trans_files WHERE phase = 'phase4_repacked')
     < (SELECT files FROM t_pack_trans_files WHERE phase = 'phase3_spilled');
SELECT 'phase4_filtered_lookup', count() FROM t_pack_trans WHERE w = '7919';
SELECT 'phase4_filtered_lookup_unknown', count() FROM t_pack_trans WHERE w = '31337';
CHECK TABLE t_pack_trans SETTINGS check_query_single_value_result = 1;

DROP TABLE t_pack_trans;
DROP TABLE t_pack_trans_files;
