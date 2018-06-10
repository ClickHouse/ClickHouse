DROP TABLE IF EXISTS test.tab;
DROP TABLE IF EXISTS test.mv;

CREATE TABLE test.tab(d Date, x UInt32) ENGINE MergeTree(d, x, 8192);
CREATE MATERIALIZED VIEW test.mv(d Date, y UInt64) ENGINE MergeTree(d, y, 8192) AS SELECT d, x + 1 AS y FROM test.tab;

INSERT INTO test.tab VALUES ('2018-01-01', 1), ('2018-01-01', 2), ('2018-02-01', 3);

SELECT '-- Before DROP PARTITION --';
SELECT * FROM test.mv ORDER BY y;

ALTER TABLE test.mv DROP PARTITION 201801;

SELECT '-- After DROP PARTITION --';
SELECT * FROM test.mv ORDER BY y;

DROP TABLE test.tab;
DROP TABLE test.mv;
