-- `RENAME COLUMN` combined with an explicit `MODIFY COLUMN ... MATERIALIZED` of a column whose
-- expansion depends on another `MATERIALIZED` column changed by the same ALTER. The automatic
-- rematerialization must follow the renames: a renamed dependent is rematerialized under its new
-- name, while a column that is explicitly modified keeps the ordinary metadata-only semantics.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- renaming and modifying the same column in one ALTER is not supported';
DROP TABLE IF EXISTS t_rename_modify_same_alter;
CREATE TABLE t_rename_modify_same_alter
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_rename_modify_same_alter
    RENAME COLUMN m2 TO m3,
    MODIFY COLUMN m3 UInt64 MATERIALIZED m1 + 10,
    ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE t_rename_modify_same_alter
    MODIFY COLUMN m2 UInt64 MATERIALIZED m1 + 10,
    RENAME COLUMN m2 TO m3; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_rename_modify_same_alter;

SELECT '-- a renamed dependent is rematerialized under its new name';
DROP TABLE IF EXISTS t_rename_dependent;
CREATE TABLE t_rename_dependent
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_rename_dependent (a) SELECT number FROM numbers(3);

-- `ADD COLUMN b` changes the expansion of `*` inside `m1` (from `greatest(a, a)` to
-- `greatest(a, a, b)` = `a + 1000`), so `m1` is rematerialized, and so is the column that reads it
-- even though the same ALTER renames it from `m2` to `m3`.
ALTER TABLE t_rename_dependent
    RENAME COLUMN m2 TO m3,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT a, m1, m3 FROM t_rename_dependent ORDER BY a;

DROP TABLE t_rename_dependent;

SELECT '-- explicit MODIFY of a renamed dependent in a later ALTER stays metadata-only';
DROP TABLE IF EXISTS t_rename_then_modify;
CREATE TABLE t_rename_then_modify
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_rename_then_modify (a) SELECT number FROM numbers(3);

ALTER TABLE t_rename_then_modify RENAME COLUMN m2 TO m3;

-- The explicit `MODIFY COLUMN` keeps `m3` metadata-only, so it is not part of the
-- rematerialization closure of `m1` and the existing parts keep the old values.
ALTER TABLE t_rename_then_modify
    MODIFY COLUMN m3 UInt64 MATERIALIZED m1 + 10,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_rename_then_modify' AND command ILIKE '%MATERIALIZE COLUMN%m3%';

SELECT a, m1, m3 FROM t_rename_then_modify ORDER BY a;

DROP TABLE t_rename_then_modify;
