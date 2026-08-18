-- CLEAR COLUMN recomputes the MATERIALIZED columns that read the cleared one. A MATERIALIZED column
-- derived from an EPHEMERAL column cannot be recomputed outside INSERT, so it must be left alone:
-- recomputing it fails to resolve the EPHEMERAL name and the whole mutation dies with
-- "Missing columns: 'e' while processing 'x + e'".

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
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple();

INSERT INTO t_clear_ephemeral (x, y, e) VALUES (1, 0, 7);
SELECT x, me, me2, mk FROM t_clear_ephemeral;

-- Only `mk` reaches the cleared column; `me` and `me2` keep their stored values.
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
