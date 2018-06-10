USE test;
DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS replacing;

CREATE TABLE test
(
    date Date,
    x Int32,
    ver UInt64
)
ENGINE = MergeTree(date, x, 4096);

INSERT INTO test VALUES ('2018-03-21', 1, 1), ('2018-03-21', 1, 2);
CREATE TABLE replacing ENGINE = ReplacingMergeTree(date, x, 4096, ver) AS SELECT * FROM test;

SELECT * FROM test ORDER BY ver;

SELECT * FROM replacing ORDER BY ver;
SELECT * FROM replacing FINAL ORDER BY ver;

OPTIMIZE TABLE replacing PARTITION '201803' FINAL;

SELECT * FROM replacing ORDER BY ver;
SELECT * FROM replacing FINAL ORDER BY ver;

DROP TABLE test;
DROP TABLE replacing;
