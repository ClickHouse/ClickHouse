-- A `MATERIALIZED` column explicitly modified by `MODIFY COLUMN ... MATERIALIZED` keeps the
-- ordinary metadata-only semantics even when the same ALTER changes the matcher expansion of
-- another `MATERIALIZED` column it depends on: the dependent-rematerialization closure must
-- not pull the explicitly modified column back into automatic rematerialization, and must not
-- reject the ALTER when that column is in the sort key.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- explicit MODIFY of a dependent stays metadata-only';
DROP TABLE IF EXISTS t_modify_dependent;
CREATE TABLE t_modify_dependent
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_modify_dependent (a) SELECT number FROM numbers(3);

-- `ADD COLUMN b` changes the expansion of `*` inside `m1` (from `greatest(a, a)` to
-- `greatest(a, a, b)` = `a + 1000`), so `m1` is rematerialized. `m2` depends on `m1` but is
-- explicitly modified by the same ALTER, so it keeps its old values (`a + 1`).
ALTER TABLE t_modify_dependent
    MODIFY COLUMN m2 UInt64 MATERIALIZED m1 + 10,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

-- Automatic rematerialization covers `m1` only, not `m2`.
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_modify_dependent' AND command ILIKE '%MATERIALIZE COLUMN%m1%';
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_modify_dependent' AND command ILIKE '%MATERIALIZE COLUMN%m2%';

SELECT a, m1, m2 FROM t_modify_dependent ORDER BY a;
-- New inserts use the new expression for `m2`.
INSERT INTO t_modify_dependent (a) VALUES (10);
SELECT a, m1, m2 FROM t_modify_dependent WHERE a = 10;

DROP TABLE t_modify_dependent;

SELECT '-- explicitly modified dependent in the sort key: ALTER is allowed';
DROP TABLE IF EXISTS t_modify_dependent_key;
CREATE TABLE t_modify_dependent_key
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY (a, m2);

INSERT INTO t_modify_dependent_key (a) SELECT number FROM numbers(3);

-- `m2` is in the sort key, but since the explicit `MODIFY COLUMN` keeps it metadata-only it is
-- not part of the rematerialization closure and the ALTER must not be rejected.
ALTER TABLE t_modify_dependent_key
    MODIFY COLUMN m2 UInt64 MATERIALIZED m1 + 10,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT a, m1, m2 FROM t_modify_dependent_key ORDER BY a;

DROP TABLE t_modify_dependent_key;
