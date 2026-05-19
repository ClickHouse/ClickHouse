-- Full-text indices are not supported by packed_skip_index_max_bytes and are always written
-- per-file. This test verifies the layout stays per-file across INSERT and OPTIMIZE FINAL,
-- and that DROP INDEX removes all skip-index bytes (a stray skp_idx.packed would leak past
-- the drop).

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_text_no_pack;
CREATE TABLE t_text_no_pack
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = 4194304,
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_text_no_pack SELECT number,     toString(number * 7919)  FROM numbers(2000);
INSERT INTO t_text_no_pack SELECT number+2000, toString(number * 31337) FROM numbers(2000);

SELECT 'after_insert_count', count() FROM t_text_no_pack;

OPTIMIZE TABLE t_text_no_pack FINAL;

SELECT 'after_merge_count', count() FROM t_text_no_pack;
SELECT 'after_merge_active_parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_no_pack' AND active;
SELECT 'after_merge_text_filtered_lookup',
    count() FROM t_text_no_pack WHERE s = '7919' SETTINGS allow_experimental_analyzer = 1;
SELECT 'after_merge_text_filters_granules', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_text_no_pack WHERE s = '7919') AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;

CHECK TABLE t_text_no_pack SETTINGS check_query_single_value_result = 1;

-- The clinching invariant: secondary_indices_compressed_bytes > 0 (text index materialized),
-- and a follow-up DROP INDEX removes ALL skip-index file bytes. With a stray skp_idx.packed
-- from the (would-be) INSERT-time packing, dropping the only text index would leave the
-- archive behind and secondary_indices_compressed_bytes would stay positive.
SELECT 'before_drop_indices_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_text_no_pack' AND active;
ALTER TABLE t_text_no_pack DROP INDEX idx_s SETTINGS mutations_sync = 2;
SELECT 'after_drop_indices_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_text_no_pack' AND active;
CHECK TABLE t_text_no_pack SETTINGS check_query_single_value_result = 1;

DROP TABLE t_text_no_pack;
