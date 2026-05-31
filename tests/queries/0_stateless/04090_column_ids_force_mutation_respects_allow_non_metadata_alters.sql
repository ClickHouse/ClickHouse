-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- Scenario: with column IDs active, `ALTER TABLE t DROP COLUMN x, ADD COLUMN x ...`
-- triggers a forced DROP+re-ADD mutation under the hood.  If the user has
-- `allow_non_metadata_alters = 0`, the column-IDs planning must respect it
-- and refuse the ALTER, not silently smuggle a data-rewrite through.
SET allow_experimental_column_ids = 1;

DROP TABLE IF EXISTS t_force_mut_guard SYNC;
CREATE TABLE t_force_mut_guard (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_force_mut_guard VALUES (1, 'hello');

-- Force-mutation case rejected when the setting forbids data-rewriting alters.
ALTER TABLE t_force_mut_guard DROP COLUMN b, ADD COLUMN b String DEFAULT 'x'
SETTINGS allow_non_metadata_alters = 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Same ALTER without the setting is allowed (force-mutation path runs).
ALTER TABLE t_force_mut_guard DROP COLUMN b, ADD COLUMN b String DEFAULT 'x';
SELECT a, b FROM t_force_mut_guard ORDER BY a;

DROP TABLE t_force_mut_guard SYNC;
