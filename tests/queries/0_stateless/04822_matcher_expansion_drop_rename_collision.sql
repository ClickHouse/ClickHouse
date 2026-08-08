-- An ALTER may drop a column and rename another one into the freed name. The renamed
-- `MATERIALIZED` column is not the dropped one: when its matcher expansion changes in the same
-- ALTER, it must still be rematerialized under its new (previously freed) name.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- a column renamed into a name freed by DROP is still rematerialized';
DROP TABLE IF EXISTS t_drop_rename_collision;
CREATE TABLE t_drop_rename_collision
(
    a UInt64,
    c UInt64,
    x UInt64 MATERIALIZED greatest(COLUMNS('^(a|b)$'))
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_drop_rename_collision (a, c) SELECT number, 5 FROM numbers(3);

SELECT a, x FROM t_drop_rename_collision ORDER BY a;

-- `ADD COLUMN b` changes the expansion of the `COLUMNS` matcher inside the renamed column (from
-- `greatest(a)` to `greatest(a, b)`), so it is rematerialized even though its post-ALTER name `c`
-- collides with the name of the column dropped by the same ALTER.
ALTER TABLE t_drop_rename_collision
    DROP COLUMN c,
    RENAME COLUMN x TO c,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_drop_rename_collision' AND command ILIKE '%MATERIALIZE COLUMN%c%';

SELECT a, b, c FROM t_drop_rename_collision ORDER BY a;

DROP TABLE t_drop_rename_collision;

SELECT '-- dropping a matcher-dependent MATERIALIZED column queues no rematerialization for it';
DROP TABLE IF EXISTS t_drop_matcher_column;
CREATE TABLE t_drop_matcher_column
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, *)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_drop_matcher_column (a) SELECT number FROM numbers(3);

ALTER TABLE t_drop_matcher_column
    DROP COLUMN m,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_drop_matcher_column' AND command ILIKE '%MATERIALIZE COLUMN%';

SELECT a, b FROM t_drop_matcher_column ORDER BY a;

DROP TABLE t_drop_matcher_column;
