-- CLEAR COLUMN recomputes the MATERIALIZED columns that read the cleared one. A MATERIALIZED column
-- derived from an EPHEMERAL column cannot be recomputed outside INSERT, so it must be left alone:
-- recomputing it fails to resolve the EPHEMERAL name and the whole mutation dies with
-- "There is no column or subcolumn e in table".

SET mutations_sync = 2;

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
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
-- The runner randomizes both of these together with `min_bytes_for_wide_part`, and on a Wide part
-- with either block column on the mutation does not recompute MATERIALIZED columns at all, so `mk`
-- would keep its pre-clear value and the recompute this test is about would never run.
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_clear_ephemeral (x, y, e) VALUES (1, 0, 7);
SELECT x, me, me2, mk FROM t_clear_ephemeral;

-- `mk` is recomputed from the cleared `x`. `me` reads the EPHEMERAL `e`, so it is absent from the
-- dependency graph the closure walks and keeps its stored value. `me2` reads only `me`, so the
-- closure starting at `x` never reaches it either -- it is not recomputed, and keeps its value too.
ALTER TABLE t_clear_ephemeral CLEAR COLUMN x IN PARTITION tuple();
SELECT x, me, me2, mk FROM t_clear_ephemeral;

-- A cleared column that no MATERIALIZED column reads must not start a recompute at all.
DROP TABLE IF EXISTS t_clear_unrelated;

CREATE TABLE t_clear_unrelated
(
    x Int32,
    y Int32,
    e Int32 EPHEMERAL 0,
    me Int32 MATERIALIZED x + e
)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple();

INSERT INTO t_clear_unrelated (x, y, e) VALUES (1, 5, 7);
ALTER TABLE t_clear_unrelated CLEAR COLUMN y IN PARTITION tuple();
SELECT x, y, me FROM t_clear_unrelated;

DROP TABLE t_clear_ephemeral;
DROP TABLE t_clear_unrelated;
