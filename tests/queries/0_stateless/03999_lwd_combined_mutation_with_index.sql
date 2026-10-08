-- Tags: no-replicated-database
-- Regression test: combined UPDATE + ALTER DELETE on a compact part with
-- a prior lightweight delete and a secondary index used to fail with
-- NOT_FOUND_COLUMN_IN_BLOCK because `affects_all_columns` was not copied
-- when computing `updated_header` in MutationsInterpreter::prepare.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_lwd_combined_mutation;

CREATE TABLE t_lwd_combined_mutation (id UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_lwd_combined_mutation SELECT number, 0 FROM numbers(100);

-- Create a lightweight delete so the part gets a _row_exists column.
DELETE FROM t_lwd_combined_mutation WHERE id % 5 = 0;

-- Combined UPDATE + ALTER DELETE in one statement (single mutation).
-- This must not throw NOT_FOUND_COLUMN_IN_BLOCK.
ALTER TABLE t_lwd_combined_mutation UPDATE v = 1 WHERE id % 4 = 0, DELETE WHERE id % 10 = 1;

SELECT count(), sum(v) FROM t_lwd_combined_mutation;
SELECT count(), sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwd_combined_mutation' AND active;

DROP TABLE IF EXISTS t_lwd_combined_mutation;
