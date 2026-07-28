-- Tags: no-random-merge-tree-settings
-- Regression test for issue #109595: a full-part-rewrite mutation (`MutateAllPartColumnsTask`)
-- hardlinks non-recalculated skip-index files into the new Wide part but used to omit their
-- checksums from checksums.txt, so `CHECK TABLE` failed with UNEXPECTED_FILE_IN_DATA_PART.

DROP TABLE IF EXISTS t_skip_idx_checksums;

CREATE TABLE t_skip_idx_checksums
(
    k UInt64,
    s String,
    v UInt64,
    m Map(String, UInt64) MATERIALIZED map('a', k),
    INDEX mm_v v TYPE minmax GRANULARITY 1,
    INDEX bf_s s TYPE bloom_filter GRANULARITY 1,
    INDEX set_v v TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
-- Force eager index-size calculation so secondary_indices_compressed_bytes is deterministic
-- across environments (lazily it depends on whether checksums are loaded at query time).
-- index_granularity = 100 over 2000 rows gives 20 granules, so the preserved minmax on the
-- monotonic non-primary-key column v can actually prune (Granules: 1/20). A single granule
-- would leave nothing to prune.
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, columns_and_secondary_indices_sizes_lazy_calculation = 0, index_granularity = 100;

INSERT INTO t_skip_idx_checksums (k, s, v) SELECT number, toString(number % 50), number FROM numbers(2000);
OPTIMIZE TABLE t_skip_idx_checksums FINAL;

SELECT part_type FROM system.parts WHERE table = 't_skip_idx_checksums' AND active AND database = currentDatabase();

-- Full part rewrite: DROP COLUMN of a MATERIALIZED column takes `MutateAllPartColumnsTask`.
-- The skip indices are not recalculated, so they are hardlinked from the source part.
ALTER TABLE t_skip_idx_checksums DROP COLUMN m SETTINGS mutations_sync = 2;

-- Indices must still be present in the new part.
SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE table = 't_skip_idx_checksums' AND active AND database = currentDatabase();

-- Their checksums must be in checksums.txt, otherwise `CHECK TABLE` fails.
CHECK TABLE t_skip_idx_checksums SETTINGS check_query_single_value_result = 0;

-- The preserved index must still be usable: it must actually prune granules (1/20), not just
-- appear by name. v is not the primary key, so only the minmax skip index can eliminate granules.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_skip_idx_checksums WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%';
SELECT count() FROM t_skip_idx_checksums WHERE v = 1042;

DROP TABLE t_skip_idx_checksums;

-- Same regression, but for a skip index whose stream filename is long enough to be stored
-- hashed via replace_long_file_name_to_hash (sipHash128String of the stream name). The old
-- fix scanned checksums by the raw skp_idx_<name> prefix and never reached the hashed files,
-- so the index was hardlinked-and-checksummed for short names only and silently dropped for
-- long names during the full-part rewrite.
DROP TABLE IF EXISTS t_skip_idx_long_name;

CREATE TABLE t_skip_idx_long_name
(
    k UInt64,
    v UInt64,
    m Map(String, UInt64) MATERIALIZED map('a', k),
    INDEX idx_with_a_very_long_name_to_force_hashing_of_the_skip_index_stream_filename_beyond_max_file_name_length_threshold_aaaaaaaaaaaaaaaa v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, columns_and_secondary_indices_sizes_lazy_calculation = 0, replace_long_file_name_to_hash = 1, max_file_name_length = 127, index_granularity = 100;

INSERT INTO t_skip_idx_long_name (k, v) SELECT number, number FROM numbers(2000);
OPTIMIZE TABLE t_skip_idx_long_name FINAL;

ALTER TABLE t_skip_idx_long_name DROP COLUMN m SETTINGS mutations_sync = 2;

-- The long-named index must still be present after the full part rewrite (0 before the fix).
SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE table = 't_skip_idx_long_name' AND active AND database = currentDatabase();

CHECK TABLE t_skip_idx_long_name SETTINGS check_query_single_value_result = 0;

-- The preserved long-named index must actually prune granules (1/20), not just appear by name.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_skip_idx_long_name WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%';
SELECT count() FROM t_skip_idx_long_name WHERE v = 1042;

DROP TABLE t_skip_idx_long_name;
