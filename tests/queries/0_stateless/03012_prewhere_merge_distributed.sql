DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_distributed;

CREATE TABLE test_local ( name String, date Date, sign Int8 ) ENGINE MergeTree PARTITION BY date ORDER BY name SETTINGS index_granularity = 8192;

CREATE TABLE test_distributed ( name String, date Date, sign Int8 ) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), test_local, rand64());

SET insert_distributed_sync = 1;

INSERT INTO test_distributed (name, date, sign) VALUES ('1', '2024-01-01', 1),('2', '2024-01-02', 1),('3', '2024-01-03', 1),('4', '2024-01-04', 1),('5', '2024-01-05', 1),('6', '2024-01-06', 1),('7', '2024-01-07', 1),('8', '2024-01-08', 1),('9', '2024-01-09', 1),('10', '2024-01-10', 1),('11', '2024-01-11', 1);

SELECT count() FROM test_distributed WHERE name GLOBAL IN ( SELECT name FROM test_distributed );

SET prefer_localhost_replica = 1;

SELECT count() FROM merge(currentDatabase(), '^test_distributed$') WHERE name GLOBAL IN ( SELECT name FROM test_distributed );
SELECT count() FROM merge(currentDatabase(), '^test_distributed$') PREWHERE name GLOBAL IN ( SELECT name FROM test_distributed );

SET prefer_localhost_replica = 0;

SELECT count() FROM merge(currentDatabase(), '^test_distributed$') WHERE name GLOBAL IN ( SELECT name FROM test_distributed );
SELECT count() FROM merge(currentDatabase(), '^test_distributed$') PREWHERE name GLOBAL IN ( SELECT name FROM test_distributed );

DROP TABLE test_local;
DROP TABLE test_distributed;

DROP TABLE IF EXISTS test_log;

CREATE TABLE test_log ( a int, b int ) ENGINE Log;

INSERT INTO test_log values (1, 2);

SELECT count() FROM merge(currentDatabase(), '^test_log$') PREWHERE a = 3; -- { serverError 182 }

DROP TABLE test_log;
