-- Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
-- Regression: pre-activation compact part with `[a, b, c]` on disk, then
-- two separate ALTERs `DROP COLUMN b` and `ADD COLUMN b` (allowed because
-- same-ALTER drop+re-add is rejected).  Without preserving the stale
-- column in `remapColumnsWithPhysicalNames` case (c), `c` would shift
-- from ordinal slot 2 to slot 1 in the in-memory list and the compact
-- reader would surface b's bytes (or a parse error) for `c`.  After the
-- fix, the slot stays at 2 and the compact stale-slot guard returns
-- defaults for the newly added `b`.

DROP TABLE IF EXISTS t_compact_dropadd SYNC;

CREATE TABLE t_compact_dropadd (a UInt64, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS
    serialization_info_version = 'with_column_ids',
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    allow_experimental_column_ids = 1;

INSERT INTO t_compact_dropadd VALUES (1, 'x', 1.5), (2, 'y', 9.9);

ALTER TABLE t_compact_dropadd DROP COLUMN b;
ALTER TABLE t_compact_dropadd ADD COLUMN b String DEFAULT 'def_b';

-- c must still come back with its original values; b must return the
-- new column's default.
SELECT a, b, c FROM t_compact_dropadd ORDER BY a;

-- system.parts_columns sanity: every present column has a defined
-- physical column_id (no torn rows).
SELECT count() AS bad_rows
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_compact_dropadd'
  AND active
  AND column_id = ''
  AND NOT startsWith(column, '_');

DROP TABLE t_compact_dropadd SYNC;
