DROP TABLE IF EXISTS test.final_test;
CREATE TABLE test.final_test (id String, version Date) ENGINE = ReplacingMergeTree(version, id, 8192);
INSERT INTO test.final_test (id, version) VALUES ('2018-01-01', '2018-01-01');
SELECT * FROM test.final_test FINAL PREWHERE id == '2018-01-02';
DROP TABLE test.final_test;
