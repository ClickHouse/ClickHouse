SET mutations_sync = 1;

-- Case 1: MATERIALIZED depends only on EPHEMERAL — UPDATE and DELETE should work
DROP TABLE IF EXISTS t_ephemeral_materialized;

CREATE TABLE t_ephemeral_materialized
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED reverse(c1),
    c3 Bool
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_ephemeral_materialized (c1, c3) VALUES ('abcdef', true);

SELECT c2, c3 FROM t_ephemeral_materialized;

ALTER TABLE t_ephemeral_materialized UPDATE c3 = false WHERE c2 = 'fedcba';
SELECT c2, c3 FROM t_ephemeral_materialized;

ALTER TABLE t_ephemeral_materialized DELETE WHERE c2 = 'fedcba';
SELECT count() FROM t_ephemeral_materialized;

DROP TABLE t_ephemeral_materialized;

-- Case 2: Table with BOTH ephemeral-dependent AND non-ephemeral MATERIALIZED columns.
-- Updating the source of the non-ephemeral one should recalculate it without
-- trying to recalculate the ephemeral-dependent one.
DROP TABLE IF EXISTS t_mixed_materialized;

CREATE TABLE t_mixed_materialized
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED reverse(c1),
    c3 String,
    c4 String MATERIALIZED upper(c3)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_mixed_materialized (c1, c3) VALUES ('abcdef', 'hello');
SELECT c2, c3, c4 FROM t_mixed_materialized;

ALTER TABLE t_mixed_materialized UPDATE c3 = 'world' WHERE c2 = 'fedcba';
SELECT c2, c3, c4 FROM t_mixed_materialized;

DROP TABLE t_mixed_materialized;

-- Case 3: Mixed-dependency MATERIALIZED column (EPHEMERAL + ordinary in same expression).
-- Updating the ordinary column succeeds but the MATERIALIZED value stays stale (by design).
DROP TABLE IF EXISTS t_mixed_dep;

CREATE TABLE t_mixed_dep
(
    c1 String EPHEMERAL,
    c3 String,
    c2 String MATERIALIZED concat(reverse(c1), '-', c3)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_mixed_dep (c1, c3) VALUES ('abcdef', 'hello');
SELECT c2, c3 FROM t_mixed_dep;

ALTER TABLE t_mixed_dep UPDATE c3 = 'world' WHERE c3 = 'hello' SETTINGS send_logs_level = 'error';
-- c2 stays stale (fedcba-hello) because it depends on EPHEMERAL c1 which cannot be re-read
SELECT c2, c3 FROM t_mixed_dep;

DROP TABLE t_mixed_dep;

-- Case 4: Same schema as the original issue #84981 (with block tracking columns)
DROP TABLE IF EXISTS t_ephemeral_lwu;

CREATE TABLE t_ephemeral_lwu
(
    c1 String EPHEMERAL,
    c2 String MATERIALIZED reverse(c1),
    c3 Bool
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_ephemeral_lwu (c1, c3) VALUES ('abcdef', true);

ALTER TABLE t_ephemeral_lwu UPDATE c3 = false WHERE c2 = 'fedcba';
SELECT c2, c3 FROM t_ephemeral_lwu;

DROP TABLE t_ephemeral_lwu;
