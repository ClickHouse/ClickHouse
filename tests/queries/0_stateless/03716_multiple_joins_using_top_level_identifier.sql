SET analyzer_compatibility_join_using_top_level_identifier = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id String, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t2 (id String, code String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t3 (id String, code String) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t1 VALUES ('a', 'v'), ('b', 'w');
INSERT INTO t2 VALUES ('b', 'c');
INSERT INTO t3 VALUES ('a_1', 'c'), ('b_1', 'd');

SET enable_analyzer = 1;

-- TODO: join_use_nulls reveals another issue in stress tests
-- Mute for now and track bug in
-- https://github.com/ClickHouse/ClickHouse/issues/87016

SELECT t1.id || '_1' AS id, t1.val
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 USING (id)
ORDER BY t1.val
SETTINGS join_use_nulls = 0
;

SELECT t2.id || '_1' AS id, t1.val
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 USING (id)
ORDER BY t1.val
SETTINGS join_use_nulls = 0
;

SELECT t1.id || t2.id || '_1' AS id, t1.val
FROM t1
INNER JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 USING (id)
ORDER BY t1.val
SETTINGS join_use_nulls = 0
; -- { serverError AMBIGUOUS_IDENTIFIER }
