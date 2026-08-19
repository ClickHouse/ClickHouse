-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending
-- Reading a MATERIALIZED column while a mutation is still pending must apply the whole chain on the
-- fly: a column that reaches the updated one only through another MATERIALIZED column has to be
-- recomputed from the pending value, not returned from its stale on-disk value.

SET alter_sync = 0, mutations_sync = 0;
SET apply_mutations_on_fly = 1;

SELECT 'on the fly';

-- With the mutation still pending, reading only the deepest column of the chain must give the same
-- value as reading the whole chain. `y` is never updated, so the part is not fully rewritten.
DROP TABLE IF EXISTS t_on_fly;

CREATE TABLE t_on_fly
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1,
    m3 Int32 MATERIALIZED m2 + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_on_fly (x, y) VALUES (10, 0);

SYSTEM STOP MERGES t_on_fly;

ALTER TABLE t_on_fly UPDATE x = 20 WHERE 1;

SELECT m1 FROM t_on_fly;
SELECT m2 FROM t_on_fly;
SELECT m3 FROM t_on_fly;
SELECT x, m1, m2, m3 FROM t_on_fly;
SELECT y FROM t_on_fly;

SYSTEM START MERGES t_on_fly;

SELECT 'on the fly, subcolumn';

-- A MATERIALIZED column defined over a subcolumn depends on the top-level column `t`, which is
-- what the pending command updates. Analysed without the subcolumn rewrite the dependency reads
-- as `t.a`, the command is filtered out of the on-fly chain, and the stale stored value is
-- returned unless the query happens to select `t` itself.
DROP TABLE IF EXISTS t_on_fly_subcolumn;

CREATE TABLE t_on_fly_subcolumn
(
    t Tuple(a Int32, b Int32),
    y Int32,
    m1 Int32 MATERIALIZED t.a + 1,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_on_fly_subcolumn (t, y) VALUES ((10, 0), 0);

SYSTEM STOP MERGES t_on_fly_subcolumn;

ALTER TABLE t_on_fly_subcolumn UPDATE t = (20, 0) WHERE 1;

SELECT y, m1 FROM t_on_fly_subcolumn;
SELECT m1 FROM t_on_fly_subcolumn;
SELECT m2 FROM t_on_fly_subcolumn;

SYSTEM START MERGES t_on_fly_subcolumn;

SELECT 'on the fly, ephemeral';

-- Reading a column downstream of an un-recalculatable one must not try to resolve the EPHEMERAL
-- column: its expression is analysed against physical columns only, where `e` does not exist.
DROP TABLE IF EXISTS t_on_fly_ephemeral;

CREATE TABLE t_on_fly_ephemeral
(
    x Int32,
    y Int32,
    e Int32 EPHEMERAL 0,
    me Int32 MATERIALIZED x + e,
    me2 Int32 MATERIALIZED me + 100
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_on_fly_ephemeral (x, y, e) VALUES (1, 0, 7);

SYSTEM STOP MERGES t_on_fly_ephemeral;

ALTER TABLE t_on_fly_ephemeral UPDATE x = 2 WHERE 1 SETTINGS send_logs_level = 'error';

SELECT me2 FROM t_on_fly_ephemeral;
SELECT me FROM t_on_fly_ephemeral;
SELECT x, me, me2 FROM t_on_fly_ephemeral;

SYSTEM START MERGES t_on_fly_ephemeral;

DROP TABLE t_on_fly;
DROP TABLE t_on_fly_subcolumn;
DROP TABLE t_on_fly_ephemeral;
