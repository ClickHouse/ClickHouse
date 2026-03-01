-- Regression test: WITH FILL columns must be preserved through LIMIT BY
-- in the actions chain, otherwise `addWithFillStepIfNeeded` fails with
-- "Filling column ... is not present in the block".
-- https://github.com/ClickHouse/ClickHouse/issues/84427

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t1 VALUES (1), (2), (3);

SELECT 1 FROM t1 ORDER BY toNullable(1+0) WITH FILL LIMIT 1 BY c0;

SELECT 1 FROM t1 ORDER BY toNullable(1+0) WITH FILL, c0 WITH FILL LIMIT 1 BY c0;

SELECT DISTINCT c0, 1 FROM t1 ORDER BY c0 WITH FILL, 1+0 LIMIT 1 BY c0;

DROP TABLE t1;
