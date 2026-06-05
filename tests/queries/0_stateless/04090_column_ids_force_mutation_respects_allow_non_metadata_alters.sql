-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- Scenario: with column IDs active, single-ALTER DROP+re-ADD of the same
-- column is rejected as crash-unsafe (the metadata commit happens before
-- the cleanup mutation starts, so a crash in that window could expose the
-- dropped column's old bytes through the re-added column).  The rejection
-- fires independently of `allow_non_metadata_alters`; the workaround is to
-- split into two separate ALTER statements (each fully durable).
SET allow_experimental_column_ids = 1;

DROP TABLE IF EXISTS t_force_mut_guard SYNC;
CREATE TABLE t_force_mut_guard (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_force_mut_guard VALUES (1, 'hello');

-- Rejected even with the permissive setting.
ALTER TABLE t_force_mut_guard DROP COLUMN b, ADD COLUMN b String DEFAULT 'x'; -- { serverError NOT_IMPLEMENTED }
-- Also rejected with the restrictive setting (no setting can override).
ALTER TABLE t_force_mut_guard DROP COLUMN b, ADD COLUMN b String DEFAULT 'x'
SETTINGS allow_non_metadata_alters = 0; -- { serverError NOT_IMPLEMENTED }

-- Two-ALTER workaround succeeds.
ALTER TABLE t_force_mut_guard DROP COLUMN b;
ALTER TABLE t_force_mut_guard ADD COLUMN b String DEFAULT 'x';
SELECT a, b FROM t_force_mut_guard ORDER BY a;

DROP TABLE t_force_mut_guard SYNC;
