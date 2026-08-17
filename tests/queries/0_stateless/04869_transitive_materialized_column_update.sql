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

SELECT 'clear column';

-- CLEAR COLUMN resets `x` to the type default, which makes the chain stale the same way an
-- UPDATE does, so it needs the same one-stage-per-level treatment. Recomputed together, `m2`
-- would read the pre-stage `m1` and `m3` the pre-stage `m2`, leaving both one step behind.
DROP TABLE IF EXISTS t_clear;

CREATE TABLE t_clear
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1,
    m3 Int32 MATERIALIZED m2 + 1
)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple();

INSERT INTO t_clear (x, y) VALUES (10, 0);
SELECT x, m1, m2, m3 FROM t_clear;

ALTER TABLE t_clear CLEAR COLUMN x IN PARTITION tuple();
SELECT x, m1, m2, m3 FROM t_clear;

-- An EPHEMERAL-derived column must be left alone here too: it cannot be recalculated outside
-- INSERT, and reaching it would fail to resolve `e`. Only `mk` is affected by clearing `x`.
DROP TABLE IF EXISTS t_clear_ephemeral;

CREATE TABLE t_clear_ephemeral
(
    x Int32,
    y Int32,
    e Int32 EPHEMERAL 0,
    me Int32 MATERIALIZED x + e,
    me2 Int32 MATERIALIZED me + 100,
    mk Int32 MATERIALIZED x + 1
)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple();

INSERT INTO t_clear_ephemeral (x, y, e) VALUES (1, 0, 7);
SELECT x, me, me2, mk FROM t_clear_ephemeral;

ALTER TABLE t_clear_ephemeral CLEAR COLUMN x IN PARTITION tuple();
SELECT x, me, me2, mk FROM t_clear_ephemeral;

SELECT 'ttl';

-- A TTL expression over a recalculated MATERIALIZED column must be re-evaluated: `d` moves from
-- 2102 to 2020-01-02 and every row becomes expired. Left stale, the rows are silently retained.

DROP TABLE IF EXISTS t_ttl;

CREATE TABLE t_ttl
(
    x Int32,
    y Int32,
    m1 Int32 MATERIALIZED x + 1,
    d Date MATERIALIZED toDate('2020-01-01') + m1
)
ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_ttl (x, y) SELECT 30000, number FROM numbers(100);

SELECT count(), max(d) FROM t_ttl;

ALTER TABLE t_ttl UPDATE x = 0 WHERE 1;

SELECT count() FROM t_ttl;

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

SELECT 'on the fly, patch part';

-- The per-level MATERIALIZED stages belong to the pending UPDATE, so they have to carry its
-- mutation version: the patch-visibility window is derived from consecutive stages' versions, and
-- without a version the patch becomes visible to the level stages, which then recompute `m1` from
-- the patched `z` and override the value the patch carries itself. The on-fly read must agree with
-- reading the materialized part plus its patch, which is `20 500 510 511` (`m1` in the patch was
-- computed from `x = 10`, the value visible when the lightweight update ran).
DROP TABLE IF EXISTS t_on_fly_patch;

CREATE TABLE t_on_fly_patch
(
    id UInt64,
    x Int32,
    z Int32,
    m1 Int32 MATERIALIZED x + z,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_on_fly_patch (id, x, z) VALUES (1, 10, 100);

SYSTEM STOP MERGES t_on_fly_patch;

SET enable_lightweight_update = 1, apply_patch_parts = 1;
ALTER TABLE t_on_fly_patch UPDATE x = 20 WHERE 1;

-- The lightweight update must not read through the pending mutation, otherwise the `m1` it stores
-- in the patch is already computed from `x = 20` and coincides with what recomputing it would give.
SET apply_mutations_on_fly = 0;
UPDATE t_on_fly_patch SET z = 500 WHERE 1;
SET apply_mutations_on_fly = 1;

SELECT x, z, m1, m2 FROM t_on_fly_patch;
SELECT m1 FROM t_on_fly_patch;
SELECT m2 FROM t_on_fly_patch;

SYSTEM START MERGES t_on_fly_patch;

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
DROP TABLE t_clear;
DROP TABLE t_clear_ephemeral;
DROP TABLE t_ttl;
DROP TABLE t_ephemeral_converging;
DROP TABLE t_on_fly;
DROP TABLE t_on_fly_subcolumn;
DROP TABLE t_on_fly_patch;
DROP TABLE t_on_fly_ephemeral;
