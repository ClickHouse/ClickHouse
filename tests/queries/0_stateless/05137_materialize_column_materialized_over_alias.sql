-- A MATERIALIZED column may be defined over an ALIAS column. An ALIAS is computed on read and never
-- stored, so its name cannot survive into the expression a mutation stage evaluates: the reference
-- has to be replaced by what the alias stands for, cast to the alias's declared type.
-- `04869_materialized_over_alias_column_mutation` covers UPDATE and on-fly reads and
-- `04869_clear_column_materialized_over_alias` covers CLEAR COLUMN; MATERIALIZE COLUMN builds its
-- own recompute stages, both for the materialized column itself and for the columns computed from
-- it, so it needs its own case.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_materialize_over_alias;

CREATE TABLE t_materialize_over_alias
(
    x Int32,
    a Int32 ALIAS x + 100,
    m Int32 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_materialize_over_alias (x) VALUES (5);
SELECT x, a, m FROM t_materialize_over_alias;

-- Changing what the alias stands for makes the recompute observable: without it `m` would keep the
-- value INSERT stored and the case could only assert that nothing is thrown.
ALTER TABLE t_materialize_over_alias MODIFY COLUMN a Int32 ALIAS x + 200;

-- Recomputing `m` has to resolve the alias, not demand a column no part holds.
ALTER TABLE t_materialize_over_alias MATERIALIZE COLUMN m;
SELECT x, a, m FROM t_materialize_over_alias;

DROP TABLE t_materialize_over_alias;

-- The expansion has to keep the alias's declared type: narrowed to the alias's UInt8, `x + 256` with
-- x = 300 is 44, while substituting the bare expression at the column's own UInt16 would give 556.
DROP TABLE IF EXISTS t_materialize_over_narrowing_alias;

CREATE TABLE t_materialize_over_narrowing_alias
(
    x UInt16,
    a UInt8 ALIAS x + 256,
    m UInt16 MATERIALIZED a
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_materialize_over_narrowing_alias (x) VALUES (300);
SELECT x, a, m FROM t_materialize_over_narrowing_alias;

ALTER TABLE t_materialize_over_narrowing_alias MATERIALIZE COLUMN m;
SELECT x, a, m FROM t_materialize_over_narrowing_alias;

DROP TABLE t_materialize_over_narrowing_alias;

-- The dependent-MATERIALIZED recompute stages go through the same expansion: materializing `c`
-- recomputes `m`, which reads `c` only through the alias `a`.
DROP TABLE IF EXISTS t_materialize_dependent_over_alias;

CREATE TABLE t_materialize_dependent_over_alias
(
    x Int32,
    c Int32 MATERIALIZED x * 2,
    a Int32 ALIAS c + 100,
    m Int32 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_materialize_dependent_over_alias (x) VALUES (5);
SELECT x, c, a, m FROM t_materialize_dependent_over_alias;

-- Metadata-only, so the stored `c` and `m` stay as INSERT computed them.
ALTER TABLE t_materialize_dependent_over_alias MODIFY COLUMN c Int32 MATERIALIZED x * 3;

ALTER TABLE t_materialize_dependent_over_alias MATERIALIZE COLUMN c;
SELECT x, c, a, m FROM t_materialize_dependent_over_alias;

DROP TABLE t_materialize_dependent_over_alias;

-- The same for the dependent stage: the alias's declared type has to survive the expansion. After
-- the recompute `c` is 256, so narrowing to the alias's UInt8 gives 0, while substituting the bare
-- `c` at the column's own UInt16 would give 256.
DROP TABLE IF EXISTS t_materialize_dependent_over_narrowing_alias;

CREATE TABLE t_materialize_dependent_over_narrowing_alias
(
    x UInt16,
    c UInt16 MATERIALIZED x,
    a UInt8 ALIAS c,
    m UInt16 MATERIALIZED a
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_materialize_dependent_over_narrowing_alias (x) VALUES (300);
SELECT x, c, a, m FROM t_materialize_dependent_over_narrowing_alias;

ALTER TABLE t_materialize_dependent_over_narrowing_alias MODIFY COLUMN c UInt16 MATERIALIZED x - 44;

ALTER TABLE t_materialize_dependent_over_narrowing_alias MATERIALIZE COLUMN c;
SELECT x, c, a, m FROM t_materialize_dependent_over_narrowing_alias;

DROP TABLE t_materialize_dependent_over_narrowing_alias;
