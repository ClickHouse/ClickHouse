-- A merge that produces an empty part still has to initialize the substreams of a column stored with
-- automatic `LowCardinality` serialization. The sample column for that is built from the declared type,
-- so it has to be converted to a `ColumnLowCardinality` first, otherwise the merge throws
-- `Bad cast from type DB::ColumnString to DB::ColumnLowCardinality`.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_auto_lc_empty_merge;
CREATE TABLE t_auto_lc_empty_merge
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    remove_empty_parts = 0;

SYSTEM STOP MERGES t_auto_lc_empty_merge;

INSERT INTO t_auto_lc_empty_merge SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);
INSERT INTO t_auto_lc_empty_merge SELECT number, 'w_' || toString(number % 8) FROM numbers(2000);

SELECT 'kind';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_empty_merge' AND active AND column = 'lc';

-- All rows are deleted, so the merge of the two parts writes no data at all.
-- `SYSTEM STOP MERGES` also stops mutations, so merges are started back before the `DELETE`.
SYSTEM START MERGES t_auto_lc_empty_merge;
DELETE FROM t_auto_lc_empty_merge WHERE 1;

OPTIMIZE TABLE t_auto_lc_empty_merge FINAL;

SELECT 'after the empty merge';
SELECT count() FROM t_auto_lc_empty_merge;
-- The merged part is a single empty wide part - the case that needs the empty block sample.
SELECT rows, part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_auto_lc_empty_merge' AND active;

DROP TABLE t_auto_lc_empty_merge;
