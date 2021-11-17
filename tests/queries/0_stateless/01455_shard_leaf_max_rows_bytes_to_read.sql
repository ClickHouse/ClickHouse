-- Tags: shard

-- Leaf limits is unreliable w/ prefer_localhost_replica=1.
-- Since in this case initial query and the query on the local node (to the
-- underlying table) has the same counters, so if query on the remote node
-- will be finished before local, then local node will already have some rows
-- read, and leaf limit will fail.
SET prefer_localhost_replica=0;

SELECT count() FROM (SELECT * FROM remote('127.0.0.1', system.numbers) LIMIT 100) SETTINGS max_rows_to_read_leaf=1; -- { serverError 158 }
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', system.numbers) LIMIT 100) SETTINGS max_bytes_to_read_leaf=1; -- { serverError 307 }
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', system.numbers) LIMIT 100) SETTINGS max_rows_to_read_leaf=100;
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', system.numbers) LIMIT 100) SETTINGS max_bytes_to_read_leaf=1000;

SELECT count() FROM (SELECT * FROM remote('127.0.0.2', system.numbers) LIMIT 100) SETTINGS max_rows_to_read_leaf=1; -- { serverError 158 }
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', system.numbers) LIMIT 100) SETTINGS max_bytes_to_read_leaf=1; -- { serverError 307 }
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', system.numbers) LIMIT 100) SETTINGS max_rows_to_read_leaf=100;
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', system.numbers) LIMIT 100) SETTINGS max_bytes_to_read_leaf=1000;

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_distributed;

CREATE TABLE test_local (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_distributed AS test_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), test_local, rand());

INSERT INTO test_local SELECT '2000-08-01', number as value from numbers(50000);

SELECT count() FROM (SELECT * FROM test_distributed) SETTINGS max_rows_to_read_leaf = 40000; -- { serverError 158 }
SELECT count() FROM (SELECT * FROM test_distributed) SETTINGS max_bytes_to_read_leaf = 40000; -- { serverError 307 }

SELECT count() FROM (SELECT * FROM test_distributed) SETTINGS max_rows_to_read = 60000; -- { serverError 158 }
SELECT count() FROM (SELECT * FROM test_distributed) SETTINGS max_rows_to_read_leaf = 60000;

SELECT count() FROM (SELECT * FROM test_distributed) SETTINGS max_bytes_to_read = 100000; -- { serverError 307 }
SELECT count() FROM (SELECT * FROM test_distributed) SETTINGS max_bytes_to_read_leaf = 100000;

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_distributed;
