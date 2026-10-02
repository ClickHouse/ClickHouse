-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation the on-fly cases need to stay pending

-- A MATERIALIZED column may be defined over an ALIAS column. An ALIAS is computed on read and never
-- stored, so its name cannot survive into the expression the mutation evaluates: the reference has to
-- be replaced by the expression it stands for. Without that, analysing the default fails to resolve
-- the alias and every mutation on the table throws UNKNOWN_IDENTIFIER, including one that updates a
-- column the expression does not mention.

DROP TABLE IF EXISTS t_materialized_over_alias SYNC;

CREATE TABLE t_materialized_over_alias
(
    x Int32,
    z Int32,
    a Int32 ALIAS x + 100,
    m Int32 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_materialized_over_alias (x, z) VALUES (10, 0);

SELECT 'stored';
SELECT x, z, a, m FROM t_materialized_over_alias;

-- Updating a column the expression does not read must not throw, and must leave `m` alone.
SELECT 'update of an unrelated column';
ALTER TABLE t_materialized_over_alias UPDATE z = 5 WHERE 1 SETTINGS mutations_sync = 2;
SELECT x, z, a, m FROM t_materialized_over_alias;

-- Updating the column the alias reads recalculates `m` through the alias.
SELECT 'update of the column behind the alias';
ALTER TABLE t_materialized_over_alias UPDATE x = 20 WHERE 1 SETTINGS mutations_sync = 2;
SELECT x, z, a, m FROM t_materialized_over_alias;

DROP TABLE t_materialized_over_alias SYNC;

-- The same value has to be reported while the mutation is still pending, from the read path.
DROP TABLE IF EXISTS t_materialized_over_alias_fly SYNC;

CREATE TABLE t_materialized_over_alias_fly
(
    x Int32,
    z Int32,
    a Int32 ALIAS x + 100,
    m Int32 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_materialized_over_alias_fly (x, z) VALUES (10, 0);
SYSTEM STOP MERGES t_materialized_over_alias_fly;

ALTER TABLE t_materialized_over_alias_fly UPDATE x = 20 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT 'on the fly, mutation pending';
SELECT x, a, m FROM t_materialized_over_alias_fly SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_materialized_over_alias_fly;
DROP TABLE t_materialized_over_alias_fly SYNC;

-- The expansion has to carry the alias's declared type. Reading `a` narrows to UInt8, so recomputing
-- `m` from the substituted expression must narrow the same way; substituting the bare `x` would make
-- the recomputed value differ from the one INSERT stored.
DROP TABLE IF EXISTS t_typed_alias SYNC;

CREATE TABLE t_typed_alias (x UInt16, a UInt8 ALIAS x, m UInt16 MATERIALIZED a)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_typed_alias (x) VALUES (300);

SELECT 'narrowing alias, stored';
SELECT x, a, m FROM t_typed_alias;

SELECT 'narrowing alias, after the mutation';
ALTER TABLE t_typed_alias UPDATE x = 301 WHERE 1 SETTINGS mutations_sync = 2;
SELECT x, a, m FROM t_typed_alias;

DROP TABLE t_typed_alias SYNC;

DROP TABLE IF EXISTS t_typed_alias_fly SYNC;

CREATE TABLE t_typed_alias_fly (x UInt16, a UInt8 ALIAS x, m UInt16 MATERIALIZED a)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_typed_alias_fly (x) VALUES (300);
SYSTEM STOP MERGES t_typed_alias_fly;

ALTER TABLE t_typed_alias_fly UPDATE x = 301 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT 'narrowing alias, mutation pending';
SELECT x, a, m FROM t_typed_alias_fly SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_typed_alias_fly;
DROP TABLE t_typed_alias_fly SYNC;
