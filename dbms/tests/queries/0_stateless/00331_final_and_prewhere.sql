DROP TABLE IF EXISTS test.replace;

CREATE TABLE test.replace ( EventDate Date,  Id UInt64,  Data String,  Version UInt32) ENGINE = ReplacingMergeTree(EventDate, Id, 8192, Version);
INSERT INTO test.replace VALUES ('2016-06-02', 1, 'version 1', 1);
INSERT INTO test.replace VALUES ('2016-06-02', 2, 'version 1', 1);
INSERT INTO test.replace VALUES ('2016-06-02', 1, 'version 0', 0);

SELECT * FROM test.replace ORDER BY Id, Version;
SELECT * FROM test.replace FINAL ORDER BY Id, Version;
SELECT * FROM test.replace FINAL WHERE Version = 0 ORDER BY Id, Version;

DROP TABLE test.replace;
