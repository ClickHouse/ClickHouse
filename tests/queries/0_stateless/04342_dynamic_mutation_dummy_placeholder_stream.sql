-- Regression for the "dummy" placeholder substream of a not-yet-written Dynamic column.
-- During a partial mutation `getColumnsForNewDataPart` records a placeholder substream for a column
-- that the mutation will write but that is not yet present in the source part. The mutation
-- stream-accounting must not resolve that placeholder against the source part's checksums: if the
-- table happens to contain an unrelated column literally named `dummy`, resolving the placeholder
-- to that column's real `dummy.bin` stream would mark the unchanged stream as skipped and drop it
-- from the new part.
-- See https://github.com/ClickHouse/ClickHouse/issues/107561

DROP TABLE IF EXISTS t_dyn_dummy;
CREATE TABLE t_dyn_dummy (id UInt64, dummy String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_dyn_dummy SELECT number, 'value_' || number FROM numbers(1000);

-- Add a Dynamic column and materialize it on the existing part via a partial mutation. The mutation
-- does not rewrite the unrelated `dummy` column, so its stream must be hardlinked into the new part
-- intact rather than skipped because of a placeholder/name collision.
ALTER TABLE t_dyn_dummy ADD COLUMN y Dynamic(max_types = 3) DEFAULT id::Int64 SETTINGS mutations_sync = 2;
ALTER TABLE t_dyn_dummy MATERIALIZE COLUMN y SETTINGS mutations_sync = 2;

-- The unchanged `dummy` column must still be present and consistent.
SELECT count(), countIf(dummy = 'value_' || toString(id)) FROM t_dyn_dummy;
SELECT dynamicType(y) AS t, count() FROM t_dyn_dummy GROUP BY t ORDER BY t;
CHECK TABLE t_dyn_dummy SETTINGS check_query_single_value_result = 1;

DROP TABLE t_dyn_dummy;
