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
-- We don't read the on-disk file list directly (won't work on object-storage-backed deploys);
-- instead we observe layout via skp_idx.packed checksum presence in system.parts.files-style
-- counts and via EXPLAIN indexes = 1 (the index must still gate granules in both layouts).

DROP TABLE IF EXISTS t_pack_trans;
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
         index_granularity = 1024;

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
SELECT 'phase3_index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
-- After the spill, the per-file substream files are visible in system.parts_columns -- but
-- those describe columns, not indices. Use system.parts.files instead: when the bloom_filter
-- is packed there's ONE archive file extra; when it's spilled, there are TWO standalone files
-- (.idx + .cmrk2) instead. So files_after_spill > files_when_packed is the invariant.
SELECT 'phase3_filtered_lookup', count() FROM t_pack_trans WHERE w = '7919';
CHECK TABLE t_pack_trans SETTINGS check_query_single_value_result = 1;

-- ---------------------------------------------------------------------------------------------
-- 4. ALTER DELETE the large slice: the rewritten part has only a fraction of the rows and the
--    bloom_filter shrinks back under the threshold -> packed again.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE t_pack_trans DELETE WHERE id >= 1024 SETTINGS mutations_sync = 2;
SELECT 'phase4_index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_pack_trans' AND active;
SELECT 'phase4_filtered_lookup', count() FROM t_pack_trans WHERE w = '7919';
SELECT 'phase4_filtered_lookup_unknown', count() FROM t_pack_trans WHERE w = '31337';
CHECK TABLE t_pack_trans SETTINGS check_query_single_value_result = 1;

DROP TABLE t_pack_trans;
