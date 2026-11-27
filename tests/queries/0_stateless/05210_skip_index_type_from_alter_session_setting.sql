-- A skip index's resolved expression type is persisted, so it has to be the type a fresh reload
-- resolves. `CREATE` analyses the index expression in the global context, and so do the merge and
-- `INSERT` rebuild producers. `FunctionConvert::getReturnTypeImpl` treats a `Dynamic` or `Variant`
-- argument as nullable when `cast_keep_nullable` is on, so `toString(v)` is `Nullable(String)` in
-- such a session and `String` at the server baseline.

SELECT '-- 1. an ALTER must not record a skip-index type from its own session settings';
DROP TABLE IF EXISTS t_ckn_index;
CREATE TABLE t_ckn_index (k UInt64, v Dynamic, INDEX idx toString(v) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
-- `index_granularity_bytes` is randomized, so pin it too: the granule count asserted below has to
-- be decided by `index_granularity` alone. Not 0, which would disable adaptive granularity, and a
-- randomized non-zero `min_bytes_for_wide_part` then makes the server warn on stderr.
-- A merge computes granularity per output block, so a small `merge_max_block_size` can leave a short
-- granule and shift that count.
SETTINGS index_granularity = 4, index_granularity_bytes = 10485760, merge_max_block_size = 8192;
-- Merges stay stopped until the `ALTER` has landed, so no merge can consume the two parts early and
-- rebuild `idx` while the recorded type is still consistent.
SYSTEM STOP MERGES t_ckn_index;
INSERT INTO t_ckn_index SELECT number, intDiv(number, 32) FROM numbers(64);
INSERT INTO t_ckn_index SELECT number, intDiv(number, 32) FROM numbers(64);
-- A column-level `MODIFY SETTING` is a `MODIFY_COLUMN` command, so it takes the full metadata path
-- and the re-derived index description becomes live. A table-level `MODIFY SETTING` would not: that
-- is a settings-only alter and the new metadata is never installed.
ALTER TABLE t_ckn_index MODIFY COLUMN k MODIFY SETTING max_compress_block_size = 16
    SETTINGS cast_keep_nullable = 1;
SYSTEM START MERGES t_ckn_index;
-- The merge rebuilds `idx` from the recorded type and aborts in
-- `MergeTreeIndexAggregatorSet::update` when that type disagrees with the one the merge resolves for
-- itself.
OPTIMIZE TABLE t_ckn_index FINAL;
-- Rows with `k` >= 32 carry `v` = 1 and that boundary is a granule edge, so `idx` prunes half the
-- granules. Asserting the pruned count is stronger than asserting that `idx` was consulted.
SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_ckn_index WHERE toString(v) = '1'
    SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0
) WHERE explain LIKE '%Granules: 16/32%';
-- `force_data_skipping_indices` raises `INDEX_NOT_USED` unless `idx` is actually consulted, so the
-- two counts only agree when the granule `idx` holds was built from the type the read side prunes
-- against.
SELECT count() FROM t_ckn_index WHERE toString(v) = '1'
    SETTINGS force_data_skipping_indices = 'idx', use_skip_indexes = 1, use_query_condition_cache = 0;
SELECT count() FROM t_ckn_index WHERE toString(v) = '1'
    SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;
DROP TABLE t_ckn_index;
