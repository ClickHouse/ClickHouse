-- Regression test: duplicate columns in result_columns_from_left_table caused
-- "Unexpected number of columns in result sample block" exception in `HashJoin::getNonJoinedBlocks`.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=99936&sha=494ca1e7a6070de924219397dd6a93afb0c714a0&name_0=PR&name_1=AST%20fuzzer%20%28amd_msan%29

DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS v2;
DROP TABLE IF EXISTS m;

CREATE TABLE d1 (key Int, value Int) ENGINE = Memory;
CREATE TABLE d2 (key Int, value Int) ENGINE = Memory;

INSERT INTO d1 VALUES (1, 10), (2, 20);
INSERT INTO d2 VALUES (1, 100), (3, 300);

CREATE VIEW v1 AS SELECT * FROM d1;
CREATE VIEW v2 AS SELECT * FROM d2;

CREATE TABLE m AS v1 ENGINE = Merge(currentDatabase(), '^(v1|v2)$');

SELECT _table, value
FROM m
INNER JOIN (SELECT value FROM d1) AS t ON m.value = t.value
ORDER BY value
SETTINGS enable_analyzer = 1;

SELECT _table, value
FROM m
RIGHT JOIN (SELECT value FROM d1) AS t ON m.value = t.value
ORDER BY value
SETTINGS enable_analyzer = 1;

DROP TABLE m;
DROP TABLE v2;
DROP TABLE v1;
DROP TABLE d2;
DROP TABLE d1;
