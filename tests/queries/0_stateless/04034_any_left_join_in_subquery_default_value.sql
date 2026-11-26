-- ANY LEFT JOIN should not be incorrectly converted to SEMI JOIN
-- when the IN set contains the default value of the right-side column type

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_filter;

CREATE TABLE t_left (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (id UInt32, data String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_filter (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_right VALUES (1, 'found');
INSERT INTO t_filter VALUES (0), (1);

SELECT t_left.id, t_right.id, t_right.data
FROM t_left ANY LEFT JOIN t_right ON t_left.id = t_right.id
WHERE t_right.id IN (SELECT id FROM t_filter)
ORDER BY t_left.id;

DROP TABLE IF EXISTS t_left_s;
DROP TABLE IF EXISTS t_right_s;
DROP TABLE IF EXISTS t_filter_s;

CREATE TABLE t_left_s (id String, value UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_s (id String, data String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_filter_s (id String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left_s VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO t_right_s VALUES ('a', 'found');
INSERT INTO t_filter_s VALUES (''), ('a');

SELECT t_left_s.id, t_right_s.id, t_right_s.data
FROM t_left_s ANY LEFT JOIN t_right_s ON t_left_s.id = t_right_s.id
WHERE t_right_s.id IN (SELECT id FROM t_filter_s)
ORDER BY t_left_s.id;

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_filter;
DROP TABLE t_left_s;
DROP TABLE t_right_s;
DROP TABLE t_filter_s;
