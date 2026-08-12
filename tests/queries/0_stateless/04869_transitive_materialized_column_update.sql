-- A MATERIALIZED column that depends on an updated column only through another MATERIALIZED
-- column must be recalculated too, and it must see the recalculated value of that column.

DROP TABLE IF EXISTS t_chain;
DROP TABLE IF EXISTS t_diamond;
DROP TABLE IF EXISTS t_ephemeral;
DROP TABLE IF EXISTS t_key;

SET mutations_sync = 2;

SELECT 'chain';

CREATE TABLE t_chain
(
    x Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1,
    m3 Int32 MATERIALIZED m2 + 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_chain (x) VALUES (10);
SELECT x, m1, m2, m3 FROM t_chain;

ALTER TABLE t_chain UPDATE x = 20 WHERE 1;
SELECT x, m1, m2, m3 FROM t_chain;

ALTER TABLE t_chain UPDATE x = 30 WHERE 1;
SELECT x, m1, m2, m3 FROM t_chain;

SELECT 'diamond';

-- m3 reads both m1 (one level away from x) and m2 (two levels away), so it has to be
-- recalculated after m2, not together with it.
CREATE TABLE t_diamond
(
    x Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 * 10,
    m3 Int32 MATERIALIZED m1 + m2
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_diamond (x) VALUES (1);
SELECT x, m1, m2, m3 FROM t_diamond;

ALTER TABLE t_diamond UPDATE x = 5 WHERE 1;
SELECT x, m1, m2, m3 FROM t_diamond;

SELECT 'ephemeral';

-- `me` cannot be recalculated during a mutation because EPHEMERAL columns are not stored,
-- so neither it nor `me2`, which reads it, may be touched.
CREATE TABLE t_ephemeral
(
    x Int32,
    e Int32 EPHEMERAL 0,
    me Int32 MATERIALIZED x + e,
    me2 Int32 MATERIALIZED me + 100
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_ephemeral (x, e) VALUES (1, 7);
SELECT x, me, me2 FROM t_ephemeral;

ALTER TABLE t_ephemeral UPDATE x = 2 WHERE 1;
SELECT x, me, me2 FROM t_ephemeral;

-- Converging paths: `mc2` reads the un-recalculatable `mc` but also the updated `x` directly.
-- Its own expression reads only stored columns, so it IS recalculated, from the stored `mc`.
DROP TABLE IF EXISTS t_ephemeral_converging;

CREATE TABLE t_ephemeral_converging
(
    x Int32,
    e Int32 EPHEMERAL 0,
    mc Int32 MATERIALIZED x + e,
    mc2 Int32 MATERIALIZED mc + x
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_ephemeral_converging (x, e) VALUES (1, 7);
SELECT x, mc, mc2 FROM t_ephemeral_converging;

ALTER TABLE t_ephemeral_converging UPDATE x = 2 WHERE 1;
SELECT x, mc, mc2 FROM t_ephemeral_converging;

SELECT 'key column';

-- Updating `x` changes the sorting key through two MATERIALIZED columns.
CREATE TABLE t_key
(
    x Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY m2;

INSERT INTO t_key (x) VALUES (1);
ALTER TABLE t_key UPDATE x = 2 WHERE 1; -- { serverError CANNOT_UPDATE_COLUMN }

SELECT 'projection';

-- A projection over a recalculated MATERIALIZED column must be rebuilt, otherwise it keeps
-- answering from the pre-mutation values. `m1` is one level away from `x`, `m2` two levels.
-- `y` is never updated, so the mutation rewrites only some columns of the part; a mutation
-- that rewrites all of them would rebuild everything anyway and hide the bug.
DROP TABLE IF EXISTS t_projection;

CREATE TABLE t_projection
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1,
    PROJECTION p1 (SELECT sum(m1)),
    PROJECTION p2 (SELECT sum(m2))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_projection (x, y) SELECT number, number FROM numbers(1000);

ALTER TABLE t_projection UPDATE x = x + 1000000 WHERE 1;

SELECT sum(m1) FROM t_projection SETTINGS optimize_use_projections = 1;
SELECT sum(m1) FROM t_projection SETTINGS optimize_use_projections = 0;
SELECT sum(m2) FROM t_projection SETTINGS optimize_use_projections = 1;
SELECT sum(m2) FROM t_projection SETTINGS optimize_use_projections = 0;

SELECT 'skip index';

DROP TABLE IF EXISTS t_index;

CREATE TABLE t_index
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1,
    INDEX idx m2 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity_bytes = 0;

INSERT INTO t_index (x, y) SELECT number, number FROM numbers(10000);

ALTER TABLE t_index UPDATE x = x + 1000000 WHERE 1;

SELECT count() FROM t_index WHERE m2 > 1000000 SETTINGS force_data_skipping_indices = 'idx';

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

SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_on_fly UPDATE x = 20 WHERE 1;

SET apply_mutations_on_fly = 1;
SELECT m1 FROM t_on_fly;
SELECT m2 FROM t_on_fly;
SELECT m3 FROM t_on_fly;
SELECT x, m1, m2, m3 FROM t_on_fly;
SELECT y FROM t_on_fly;

SYSTEM START MERGES t_on_fly;

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

DROP TABLE t_chain;
DROP TABLE t_diamond;
DROP TABLE t_ephemeral;
DROP TABLE t_key;
DROP TABLE t_projection;
DROP TABLE t_index;
DROP TABLE t_ephemeral_converging;
DROP TABLE t_on_fly;
DROP TABLE t_on_fly_ephemeral;
