-- A `MODIFY COLUMN` that only changes the position or the type of a `MATERIALIZED` column keeps
-- its stored expression, so it must not suppress the automatic rematerialization that another
-- command of the same ALTER triggers by changing the matcher expansion of that column. Only an
-- explicit `MODIFY COLUMN ... MATERIALIZED <other expression>` keeps the metadata-only semantics
-- (covered by `04627_matcher_expansion_modify_materialized_same_alter`).

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- positional MODIFY of the column whose expansion changes';
DROP TABLE IF EXISTS t_positional_modify;
CREATE TABLE t_positional_modify
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_positional_modify (a) SELECT number FROM numbers(3);

-- `ADD COLUMN b` changes the expansion of `*` inside `m` from `greatest(a, a)` to
-- `greatest(a, a, b)`, and `MODIFY COLUMN m UInt64 AFTER a` only reorders `m`.
ALTER TABLE t_positional_modify
    MODIFY COLUMN m UInt64 AFTER a,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_positional_modify' AND command ILIKE '%MATERIALIZE COLUMN%m%';

SELECT a, m, b FROM t_positional_modify ORDER BY a;
-- Existing parts hold the same values a new insert would produce.
INSERT INTO t_positional_modify (a) VALUES (10);
SELECT a, m, b FROM t_positional_modify WHERE a = 10;

DROP TABLE t_positional_modify;

SELECT '-- type-only MODIFY of the column whose expansion changes';
DROP TABLE IF EXISTS t_type_modify;
CREATE TABLE t_type_modify
(
    a UInt64,
    m UInt32 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_type_modify (a) SELECT number FROM numbers(3);

ALTER TABLE t_type_modify
    MODIFY COLUMN m UInt64,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_type_modify' AND command ILIKE '%MATERIALIZE COLUMN%m%';

SELECT a, m, b FROM t_type_modify ORDER BY a;
INSERT INTO t_type_modify (a) VALUES (10);
SELECT a, m, b FROM t_type_modify WHERE a = 10;

DROP TABLE t_type_modify;

SELECT '-- a positionally modified dependent stays in the closure';
DROP TABLE IF EXISTS t_positional_dependent;
CREATE TABLE t_positional_dependent
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_positional_dependent (a) SELECT number FROM numbers(3);

-- `m1` is rematerialized because `ADD COLUMN b` extends its matcher, and `m2` reads `m1` while
-- being only reordered, so it must be rematerialized as well.
ALTER TABLE t_positional_dependent
    MODIFY COLUMN m2 UInt64 FIRST,
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_positional_dependent' AND command ILIKE '%MATERIALIZE COLUMN%m2%';

SELECT a, m1, m2, b FROM t_positional_dependent ORDER BY a;
INSERT INTO t_positional_dependent (a) VALUES (10);
SELECT a, m1, m2, b FROM t_positional_dependent WHERE a = 10;

DROP TABLE t_positional_dependent;

SELECT '-- restating the same expression does not suppress it either';
DROP TABLE IF EXISTS t_same_expression;
CREATE TABLE t_same_expression
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_same_expression (a) SELECT number FROM numbers(3);

ALTER TABLE t_same_expression
    MODIFY COLUMN m UInt64 MATERIALIZED greatest(a, * EXCEPT m),
    ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_same_expression' AND command ILIKE '%MATERIALIZE COLUMN%m%';

SELECT a, m, b FROM t_same_expression ORDER BY a;

DROP TABLE t_same_expression;
