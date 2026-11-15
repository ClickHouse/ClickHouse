-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: needs the JSON type.
-- no-random-merge-tree-settings: the scenario needs a wide part so the hint stays unmaterialized and the
--   part carries secondary_index_column_types.json; randomized settings could force a compact part.

SET enable_json_lazy_type_hints = 1;

-- A wide part carries secondary_index_column_types.json when a skip index is materialized over a lazy
-- JSON hint (the column stays Dynamic on disk while the granules use the hinted type). A later
-- some-columns mutation that recomputes an empty record - here dropping the last (and only) index that
-- needed the override - must drop the stale checksums.txt entry for that file. Otherwise the new part
-- references a file it does not have and fails a consistency check.
DROP TABLE IF EXISTS t_idx_types_cleanup;
CREATE TABLE t_idx_types_cleanup (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_idx_types_cleanup SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
ALTER TABLE t_idx_types_cleanup MODIFY COLUMN j JSON(a UInt64) SETTINGS alter_sync = 2;
ALTER TABLE t_idx_types_cleanup ADD INDEX idx j.a TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_idx_types_cleanup MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

-- The index prunes while the record is present.
SELECT count() FROM t_idx_types_cleanup WHERE j.a = 30;

-- Drop the only index: the mutation recomputes an empty record for the part.
ALTER TABLE t_idx_types_cleanup DROP INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;

-- The part must be consistent: checksums.txt must not reference the removed secondary_index_column_types.json.
SELECT count() FROM t_idx_types_cleanup WHERE j.a = 30;
CHECK TABLE t_idx_types_cleanup SETTINGS check_query_single_value_result = 1;

-- A fresh load re-validates part checksums against the files on disk, so it must succeed too.
DETACH TABLE t_idx_types_cleanup;
ATTACH TABLE t_idx_types_cleanup;
SELECT count() FROM t_idx_types_cleanup;
DROP TABLE t_idx_types_cleanup;
