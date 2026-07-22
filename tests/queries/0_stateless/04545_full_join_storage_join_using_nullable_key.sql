-- { echo }
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_left_04545;
DROP TABLE IF EXISTS t_full_04545;
DROP TABLE IF EXISTS t_leftj_04545;
DROP TABLE IF EXISTS t_rightj_04545;

CREATE TABLE t_left_04545 (x Nullable(UInt32), str String) ENGINE = Memory;
CREATE TABLE t_full_04545 (x UInt32, s String) ENGINE = Join(ALL, FULL, x);

INSERT INTO t_left_04545 VALUES (1, 'a'), (2, 'b'), (NULL, 'null_row'), (0, 'left_zero');
INSERT INTO t_full_04545 VALUES (1, 'x'), (3, 'z');

-- FULL JOIN with a StorageJoin key type mismatch (Nullable(UInt32) vs UInt32). An unmatched
-- left row with a NULL key must keep NULL (not the Nullable(0) produced by casting the filled
-- UInt32 default after the join). A real left key 0 that is unmatched must stay 0.
SELECT x, isNull(x) AS x_is_null, str, s
FROM t_left_04545 FULL JOIN t_full_04545 USING (x)
ORDER BY str, x;

CREATE TABLE t_leftj_04545 (x UInt32, s String) ENGINE = Join(ALL, LEFT, x);
INSERT INTO t_leftj_04545 VALUES (1, 'x'), (3, 'z');

-- LEFT JOIN resolves the key from the left side only; the NULL key must survive.
SELECT x, isNull(x) AS x_is_null, str
FROM t_left_04545 LEFT JOIN t_leftj_04545 USING (x)
ORDER BY str, x;

CREATE TABLE t_rightj_04545 (x UInt32, s String) ENGINE = Join(ALL, RIGHT, x);
INSERT INTO t_rightj_04545 VALUES (1, 'x'), (3, 'z');

-- RIGHT JOIN resolves the key from the right (storage) side; right-only keys are kept.
SELECT x, isNull(x) AS x_is_null, s
FROM t_left_04545 RIGHT JOIN t_rightj_04545 USING (x)
ORDER BY s, x;

DROP TABLE t_left_04545;
CREATE TABLE t_left_04545 (x Nullable(UInt32), str String) ENGINE = Memory;
INSERT INTO t_left_04545 VALUES (1, 'a'), (2, 'b');

-- FULL JOIN ... ON selects the raw right key: it is not USING-coalesced, so it must keep the
-- storage type and its unmatched-left default (0), not be promoted to Nullable.
SELECT t_full_04545.x AS rx, isNull(rx) AS rx_is_null
FROM t_left_04545 FULL JOIN t_full_04545 ON t_left_04545.x = t_full_04545.x
ORDER BY rx;

DROP TABLE t_left_04545;
DROP TABLE t_full_04545;
DROP TABLE t_leftj_04545;
DROP TABLE t_rightj_04545;
