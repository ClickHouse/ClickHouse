-- Automatic rematerialization of a `MATERIALIZED` column that is renamed by the same ALTER
-- must fully work on wide parts, where untouched columns are hardlinked and renamed at the
-- file level: the recomputed column must be written fresh instead of inheriting the
-- renamed-from column's files.

SET alter_sync = 2;
SET mutations_sync = 2;
SET check_query_single_value_result = 0;

SELECT '-- rename into a name freed by DROP, wide part';
DROP TABLE IF EXISTS t_rematerialize_wide;
CREATE TABLE t_rematerialize_wide
(
    a UInt64,
    c UInt64,
    x UInt64 MATERIALIZED greatest(COLUMNS('^(a|b)$'))
) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_rematerialize_wide (a, c) SELECT number, 5 FROM numbers(3);

ALTER TABLE t_rematerialize_wide
    DROP COLUMN c,
    RENAME COLUMN x TO c,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT a, b, c FROM t_rematerialize_wide ORDER BY a;
CHECK TABLE t_rematerialize_wide;

DROP TABLE t_rematerialize_wide;

SELECT '-- plain rename, wide part';
DROP TABLE IF EXISTS t_rename_wide;
CREATE TABLE t_rename_wide
(
    a UInt64,
    x UInt64 MATERIALIZED greatest(COLUMNS('^(a|b)$'))
) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_rename_wide (a) SELECT number FROM numbers(3);

ALTER TABLE t_rename_wide
    RENAME COLUMN x TO c,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT a, b, c FROM t_rename_wide ORDER BY a;
CHECK TABLE t_rename_wide;

DROP TABLE t_rename_wide;
