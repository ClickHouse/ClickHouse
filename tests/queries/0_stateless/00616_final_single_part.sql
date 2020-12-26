SET optimize_on_insert = 0;

DROP TABLE IF EXISTS test_00616;
DROP TABLE IF EXISTS replacing_00616;

CREATE TABLE test_00616
(
    date Date,
    x Int32,
    ver UInt64
)
ENGINE = MergeTree(date, x, 4096);

INSERT INTO test_00616 VALUES ('2018-03-21', 1, 1), ('2018-03-21', 1, 2);
CREATE TABLE replacing_00616 ENGINE = ReplacingMergeTree(date, x, 4096, ver) AS SELECT * FROM test_00616;

SELECT * FROM test_00616 ORDER BY ver;

SELECT * FROM replacing_00616 ORDER BY ver;
SELECT * FROM replacing_00616 FINAL ORDER BY ver;

OPTIMIZE TABLE replacing_00616 PARTITION '201803' FINAL;

SELECT * FROM replacing_00616 ORDER BY ver;
SELECT * FROM replacing_00616 FINAL ORDER BY ver;

DROP TABLE test_00616;
DROP TABLE replacing_00616;
