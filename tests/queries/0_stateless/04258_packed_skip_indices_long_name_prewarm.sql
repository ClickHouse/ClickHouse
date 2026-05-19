-- Long-named packed substreams get a hashed name on disk (via replace_long_file_name_to_hash)
-- when written per-file, but stay under the unhashed "logical" name inside skp_idx.packed
-- (no FS name limit there). The reader resolves a packed substream's marks under the logical
-- name (checksums.txt has no per-substream entry to disambiguate against), while the writer
-- emplaces cached_index_marks under the on-disk hashed name during accumulation. Unless the
-- writer re-keys at finalize, prewarm puts marks under a key the reader never looks up.
--
-- We can't observe MarkCache hits directly from SQL, but we can verify correctness end-to-end
-- with prewarm_mark_cache enabled: the merged part's long-name packed index must (a) survive
-- mark-cache prewarm without losing entries, (b) still gate granules, and (c) CHECK TABLE.

DROP TABLE IF EXISTS t_long_pack;
CREATE TABLE t_long_pack
(
    id UInt64,
    v UInt64,
    INDEX a_very_long_index_name_that_must_be_hashed_when_stored_per_file v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = 4194304,
         replace_long_file_name_to_hash = 1,
         max_file_name_length = 64,
         auto_statistics_types = '',
         index_granularity = 1024,
         prewarm_mark_cache = 1;

INSERT INTO t_long_pack SELECT number,         number * 7  FROM numbers(2000);
INSERT INTO t_long_pack SELECT number + 2000,  number * 11 FROM numbers(2000);

-- Force the merge to exercise the prewarm path on the active part.
OPTIMIZE TABLE t_long_pack FINAL;

SELECT 'rows', count() FROM t_long_pack;
SELECT 'index_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_long_pack' AND active;

-- The skip index gates granules under both the hashed-on-disk and the logical packed-virtual
-- name. The mark-cache rekey only matters for prewarm hits, but the lookup must still find
-- marks either way, so this query must return the right count.
SELECT 'pk_match', count() FROM t_long_pack WHERE v BETWEEN 100 AND 200;
SELECT 'index_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_long_pack WHERE v = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;

CHECK TABLE t_long_pack SETTINGS check_query_single_value_result = 1;

DROP TABLE t_long_pack;
