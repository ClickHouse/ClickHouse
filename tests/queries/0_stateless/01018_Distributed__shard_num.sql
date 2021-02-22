-- FIXME: Non equi join are not allowed since #15162 (blocked by #8802), hence we expect 403 error.

DROP TABLE IF EXISTS mem1;
DROP TABLE IF EXISTS dist_1;
DROP TABLE IF EXISTS mem2;
DROP TABLE IF EXISTS dist_2;
DROP TABLE IF EXISTS mem3;
DROP TABLE IF EXISTS dist_3;

-- make the order static
SET max_threads = 1;
SET max_distributed_connections = 1;

-- remote(system.one)
SELECT 'remote(system.one)';
SELECT * FROM remote('127.0.0.1', system.one);
SELECT * FROM remote('127.0.0.{1,2}', system.one);
SELECT _shard_num, * FROM remote('127.0.0.1', system.one);
SELECT _shard_num, * FROM remote('127.0.0.{1,2}', system.one) order by _shard_num;
SELECT _shard_num, * FROM remote('127.0.0.{1,2}', system.one) WHERE _shard_num = 1;

-- dist_1 using test_shard_localhost
SELECT 'dist_1';
CREATE TABLE mem1 (key Int) Engine=Memory();
CREATE TABLE dist_1 AS mem1 Engine=Distributed(test_shard_localhost, currentDatabase(), mem1);
SELECT _shard_num FROM dist_1 order by _shard_num;

INSERT INTO mem1 VALUES (10);
SELECT _shard_num FROM dist_1 order by _shard_num;
SELECT _shard_num, key FROM dist_1 order by _shard_num;
SELECT key FROM dist_1;

INSERT INTO dist_1 VALUES (20);
SELECT _shard_num FROM dist_1 order by _shard_num;
SELECT _shard_num, key FROM dist_1 order by _shard_num, key;
SELECT key FROM dist_1;

-- dist_2 using test_cluster_two_shards_localhost
SELECT 'dist_2';
CREATE TABLE mem2 (key Int) Engine=Memory();
CREATE TABLE dist_2 AS mem2 Engine=Distributed(test_cluster_two_shards_localhost, currentDatabase(), mem2);
SELECT _shard_num FROM dist_2 order by _shard_num;

INSERT INTO mem2 VALUES (100);
SELECT _shard_num FROM dist_2 order by _shard_num;
SELECT _shard_num, key FROM dist_2 order by _shard_num, key;
SELECT key FROM dist_2;

-- multiple _shard_num
SELECT 'remote(Distributed)';
SELECT _shard_num, key FROM remote('127.0.0.1', currentDatabase(), dist_2) order by _shard_num, key;

-- JOIN system.clusters
SELECT 'JOIN system.clusters';

SELECT a._shard_num, a.key, b.host_name, b.host_address, b.port
FROM (SELECT *, _shard_num FROM dist_1) a
JOIN system.clusters b
ON a._shard_num = b.shard_num
WHERE b.cluster = 'test_cluster_two_shards_localhost';

SELECT _shard_num, key, b.host_name, b.host_address, b.port
FROM dist_1 a
JOIN system.clusters b
ON _shard_num = b.shard_num
WHERE b.cluster = 'test_cluster_two_shards_localhost'; -- { serverError 403 }

-- rewrite does not work with aliases, hence Missing columns (47)
SELECT a._shard_num, key FROM dist_1 a; -- { serverError 47; }
-- the same with JOIN, just in case
SELECT a._shard_num, a.key, b.host_name, b.host_address, b.port
FROM dist_1 a
JOIN system.clusters b
ON a._shard_num = b.shard_num
WHERE b.cluster = 'test_cluster_two_shards_localhost'; -- { serverError 47; }

SELECT 'dist_3';
CREATE TABLE mem3 (key Int, _shard_num String) Engine=Memory();
CREATE TABLE dist_3 AS mem3 Engine=Distributed(test_shard_localhost, currentDatabase(), mem3);
INSERT INTO mem3 VALUES (100, 'foo');
SELECT * FROM dist_3;
SELECT _shard_num, * FROM dist_3 order by _shard_num;

-- GROUP BY _shard_num
SELECT _shard_num, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num;
-- with alias
SELECT _shard_num s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num;
-- by alias
SELECT _shard_num s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY s;

-- GROUP BY + JOIN
SELECT any(c.host_name), _shard_num, count()
FROM cluster(test_cluster_two_shards, system.one) a
JOIN system.clusters c ON _shard_num = c.shard_num AND c.cluster = 'test_cluster_two_shards'
GROUP BY _shard_num; -- { serverError 403 }
SELECT c.host_name, _shard_num
FROM cluster(test_cluster_two_shards, system.one) a
JOIN system.clusters c ON _shard_num = c.shard_num AND c.cluster = 'test_cluster_two_shards'; -- { serverError 403 }
-- with alias
SELECT c.host_name, _shard_num s
FROM cluster(test_cluster_two_shards, system.one) a
JOIN system.clusters c ON _shard_num = c.shard_num AND c.cluster = 'test_cluster_two_shards'; -- { serverError 403 }
SELECT c.host_name, _shard_num s
FROM cluster(test_cluster_two_shards, system.one) a
JOIN system.clusters c ON s = c.shard_num AND c.cluster = 'test_cluster_two_shards'; -- { serverError 403 }
-- with group by from the right table
SELECT c.host_name, any(_shard_num) s
FROM cluster(test_cluster_two_shards, system.one) a
JOIN system.clusters c ON _shard_num = c.shard_num AND c.cluster = 'test_cluster_two_shards'
GROUP BY c.host_name; -- { serverError 403 }

-- LIMIT BY
select _shard_num, * from remote('127.{1,2}', system.one) limit 1 by dummy;
