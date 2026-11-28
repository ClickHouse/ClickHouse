-- Tags: no-fasttest
-- Prefix-collision skip-index resolution on PACKED part storage. Index `idx_a` is a name-prefix of
-- `idx_a6`; only `idx_a6` is materialized (materialize_skip_indexes_on_insert = 0). A later all-columns
-- rewrite (MATERIALIZE INDEX on a compact + packed part) must resolve `idx_a` from its own (absent)
-- files, not re-select `idx_a6`'s via a prefix scan. Pre-fix the prefix scan hardlinked idx_a6's packed
-- file for idx_a -> NOT_IMPLEMENTED on packed storage. Post-fix: CHECK TABLE passes.
SET materialize_skip_indexes_on_insert = 0;
DROP TABLE IF EXISTS t_packed_prefix_absent;

CREATE TABLE t_packed_prefix_absent
(
    k UInt64,
    v UInt64,
    w UInt64,
    INDEX idx_a       v TYPE minmax GRANULARITY 1,
    INDEX idx_a6      v TYPE minmax GRANULARITY 1,
    INDEX idx_trigger w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS min_bytes_for_full_part_storage = '1G',
         min_bytes_for_wide_part = '1G',
         packed_skip_index_max_bytes = 0,
         replace_long_file_name_to_hash = 0,
         index_granularity = 1024;

INSERT INTO t_packed_prefix_absent SELECT number, number * 7, number * 3 FROM numbers(4000);

ALTER TABLE t_packed_prefix_absent MATERIALIZE INDEX idx_a6 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed_prefix_absent MATERIALIZE INDEX idx_trigger SETTINGS mutations_sync = 2;

CHECK TABLE t_packed_prefix_absent SETTINGS check_query_single_value_result = 1;
DROP TABLE t_packed_prefix_absent;
