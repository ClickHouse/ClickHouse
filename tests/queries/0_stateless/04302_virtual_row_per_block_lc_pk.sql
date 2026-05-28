-- Tags: no-random-merge-tree-settings, no-random-settings, long
-- Regression for the LowCardinality(...) primary key path of
-- `MergeTreeSelectProcessor::buildVirtualRowFromChunk`. Before the fix, the
-- per-block virtual row code materialized the source PK column via
-- `convertToFullIfNeeded` and then called `insertFrom` into a destination
-- `cloneEmpty`-d from `pk_block_header`. When the PK type is
-- `LowCardinality(...)` the destination is `ColumnLowCardinality` while the
-- materialized source is the nested full column, so `insertFrom`'s type check
-- fires and the query exits with an exception (release builds would otherwise
-- silently corrupt the per-block boundary). The fix round-trips through
-- `Field`, which works regardless of column representation.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_vrow_lc_str;
DROP TABLE IF EXISTS t_vrow_lc_int;

CREATE TABLE t_vrow_lc_str (lc_key LowCardinality(String), v UInt32)
ENGINE = MergeTree ORDER BY lc_key
SETTINGS index_granularity = 28454, min_bytes_for_wide_part = 0;

CREATE TABLE t_vrow_lc_int (lc_key LowCardinality(Int32), v UInt32)
ENGINE = MergeTree ORDER BY lc_key
SETTINGS index_granularity = 28454, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_vrow_lc_str;
SYSTEM STOP MERGES t_vrow_lc_int;

INSERT INTO t_vrow_lc_str SELECT toString(sipHash64(number, 0) % 1000), number FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_lc_str SELECT toString(sipHash64(number, 1) % 1000), number FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_lc_str SELECT toString(sipHash64(number, 2) % 1000), number FROM numbers(100000) SETTINGS max_block_size = 14633;

INSERT INTO t_vrow_lc_int SELECT (sipHash64(number, 0) % 1000)::Int32, number FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_lc_int SELECT (sipHash64(number, 1) % 1000)::Int32, number FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_lc_int SELECT (sipHash64(number, 2) % 1000)::Int32, number FROM numbers(100000) SETTINGS max_block_size = 14633;

SET optimize_read_in_order = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1,
    max_block_size = 14633;

-- Both directions on `LowCardinality(String)` and `LowCardinality(Int32)` PKs
-- must succeed and return monotonically sorted output up to the LIMIT.

WITH (SELECT groupArray(lc_key) FROM (SELECT lc_key FROM t_vrow_lc_str ORDER BY lc_key DESC LIMIT 100)) AS arr
SELECT 'lc_string desc', length(arr) = 100, arr = arrayReverseSort(arr);

WITH (SELECT groupArray(lc_key) FROM (SELECT lc_key FROM t_vrow_lc_str ORDER BY lc_key ASC LIMIT 100)) AS arr
SELECT 'lc_string asc',  length(arr) = 100, arr = arraySort(arr);

WITH (SELECT groupArray(lc_key) FROM (SELECT lc_key FROM t_vrow_lc_int ORDER BY lc_key DESC LIMIT 100)) AS arr
SELECT 'lc_int32 desc',  length(arr) = 100, arr = arrayReverseSort(arr);

WITH (SELECT groupArray(lc_key) FROM (SELECT lc_key FROM t_vrow_lc_int ORDER BY lc_key ASC LIMIT 100)) AS arr
SELECT 'lc_int32 asc',   length(arr) = 100, arr = arraySort(arr);

DROP TABLE t_vrow_lc_str;
DROP TABLE t_vrow_lc_int;
