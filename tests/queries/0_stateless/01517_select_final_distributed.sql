-- Tags: distributed

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS test5346;

CREATE TABLE test5346 (`Id` String, `Timestamp` DateTime, `updated` DateTime)
ENGINE = ReplacingMergeTree(updated) PARTITION BY tuple() ORDER BY (Timestamp, Id);

INSERT INTO test5346 VALUES('1',toDateTime('2020-01-01 00:00:00'),toDateTime('2020-01-01 00:00:00'));

SELECT Id, Timestamp
FROM remote('localhost,127.0.0.1,127.0.0.2',currentDatabase(),'test5346') FINAL
ORDER BY Timestamp;

SELECT Id, Timestamp
FROM remote('localhost,127.0.0.1,127.0.0.2',currentDatabase(),'test5346') FINAL
ORDER BY identity(Timestamp);

DROP TABLE test5346;
