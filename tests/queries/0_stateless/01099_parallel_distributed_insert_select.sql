-- Tags: distributed

-- <Warning> ConnectionPoolWithFailover: Connection failed at try №1 - is not a problem
SET send_logs_level = 'fatal';

SET prefer_localhost_replica = 1;

DROP TABLE IF EXISTS local_01099_a;
DROP TABLE IF EXISTS local_01099_b;
DROP TABLE IF EXISTS distributed_01099_a;
DROP TABLE IF EXISTS distributed_01099_b;

SET parallel_distributed_insert_select=1;
SELECT 'parallel_distributed_insert_select=1';

--
-- test_shard_localhost
--

SELECT 'test_shard_localhost';

CREATE TABLE local_01099_a (number UInt64) ENGINE = Log;
CREATE TABLE local_01099_b (number UInt64) ENGINE = Log;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT * FROM distributed_01099_b;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--
-- test_cluster_two_shards_localhost
--

SELECT 'test_cluster_two_shards_localhost';

CREATE TABLE local_01099_a (number UInt64) ENGINE = Log;
CREATE TABLE local_01099_b (number UInt64) ENGINE = Log;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--
-- test_cluster_two_shards
--

SELECT 'test_cluster_two_shards';

CREATE TABLE local_01099_a (number UInt64) ENGINE = Log;
CREATE TABLE local_01099_b (number UInt64) ENGINE = Log;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;
SET prefer_localhost_replica=1;

-- distributed sends disabled, 0 rows (since parallel_distributed_insert_select=1)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SYSTEM FLUSH DISTRIBUTED distributed_01099_b;

SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--- test_cluster_1_shard_3_replicas_1_unavailable

SELECT 'test_cluster_1_shard_3_replicas_1_unavailable';

CREATE TABLE local_01099_a (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE local_01099_b (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;
SET prefer_localhost_replica=1;

-- distributed sends disabled, but they are not required, since insert is done into local table.
-- (since parallel_distributed_insert_select=2)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
SET send_logs_level='fatal';
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;
SET send_logs_level='warning';

--- test_cluster_1_shard_3_replicas_1_unavailable with storageCluster

SELECT 'test_cluster_1_shard_3_replicas_1_unavailable with storageCluster';

CREATE TABLE local_01099_b (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
SET send_logs_level='error';
INSERT INTO distributed_01099_b SELECT * FROM urlCluster('test_cluster_two_shards', 'http://localhost:8123/?query=select+{1,2,3}+format+TSV', 'TSV', 's String');
SET send_logs_level='warning';
SET prefer_localhost_replica=1;

-- distributed sends disabled, but they are not required, since insert is done into local table.
-- (since parallel_distributed_insert_select=2)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_b;
SET send_logs_level='fatal';
DROP TABLE distributed_01099_b;
SET send_logs_level='warning';

SET parallel_distributed_insert_select=2;
SELECT 'parallel_distributed_insert_select=2';

--
-- test_shard_localhost
--

SELECT 'test_shard_localhost';

CREATE TABLE local_01099_a (number UInt64) ENGINE = Log;
CREATE TABLE local_01099_b (number UInt64) ENGINE = Log;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT * FROM distributed_01099_b;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--- https://github.com/ClickHouse/ClickHouse/issues/78464
CREATE TABLE local_01099_c (n UInt64) ENGINE = Log;
CREATE TABLE distributed_01099_c AS local_01099_c ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_01099_c, rand());

INSERT INTO TABLE FUNCTION clusterAllReplicas('test_shard_localhost', currentDatabase(), 'distributed_01099_c') (n) SELECT number FROM remote('localhost', numbers(5)) tx;

SELECT * FROM distributed_01099_c;

DROP TABLE local_01099_c;
DROP TABLE distributed_01099_c;

--
-- test_cluster_two_shards_localhost
--

SELECT 'test_cluster_two_shards_localhost';

-- Log engine will lead to deadlock:
--     DB::Exception: std::system_error: Resource deadlock avoided.
-- So use MergeTree instead.
CREATE TABLE local_01099_a (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE local_01099_b (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_01099_b, rand());

INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;

SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--
-- test_cluster_two_shards
--

SELECT 'test_cluster_two_shards';

CREATE TABLE local_01099_a (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE local_01099_b (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;
SET prefer_localhost_replica=1;

-- distributed sends disabled, but they are not required, since insert is done into local table.
-- (since parallel_distributed_insert_select=2)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--- test_cluster_1_shard_3_replicas_1_unavailable
SET send_logs_level='error';

SELECT 'test_cluster_1_shard_3_replicas_1_unavailable';

CREATE TABLE local_01099_a (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE local_01099_b (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE distributed_01099_a AS local_01099_a ENGINE = Distributed('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), local_01099_a, rand());
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
INSERT INTO local_01099_a SELECT number from system.numbers limit 3;
INSERT INTO distributed_01099_b SELECT * from distributed_01099_a;
SET prefer_localhost_replica=1;

-- distributed sends disabled, but they are not required, since insert is done into local table.
-- (since parallel_distributed_insert_select=2)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_a;
DROP TABLE local_01099_b;
DROP TABLE distributed_01099_a;
DROP TABLE distributed_01099_b;

--- test_cluster_1_shard_3_replicas_1_unavailable with storageCluster

SELECT 'test_cluster_1_shard_3_replicas_1_unavailable with storageCluster';

CREATE TABLE local_01099_b (number UInt64) ENGINE = MergeTree() ORDER BY number;
CREATE TABLE distributed_01099_b AS local_01099_b ENGINE = Distributed('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), local_01099_b, rand());

SYSTEM STOP DISTRIBUTED SENDS distributed_01099_b;
SET prefer_localhost_replica=0; -- to require distributed send for local replica too
SET send_logs_level='error';
INSERT INTO distributed_01099_b SELECT * FROM urlCluster('test_cluster_two_shards', 'http://localhost:8123/?query=select+{1,2,3}+format+TSV', 'TSV', 's String');
SET send_logs_level='warning';
SET prefer_localhost_replica=1;

-- distributed sends disabled, but they are not required, since insert is done into local table.
-- (since parallel_distributed_insert_select=2)
SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

truncate table local_01099_b;

SET send_logs_level='error';
INSERT INTO distributed_01099_b with 'http://localhost:8123/?query=' || 'select+{1,2,3}+format+TSV' as url SELECT * FROM urlCluster('test_cluster_two_shards', (select url), 'TSV', 's String');
SET send_logs_level='warning';

SELECT 'distributed';
SELECT number, count(number) FROM distributed_01099_b group by number order by number;
SELECT 'local';
SELECT number, count(number) FROM local_01099_b group by number order by number;

DROP TABLE local_01099_b;
SET send_logs_level='fatal';
DROP TABLE distributed_01099_b;
SET send_logs_level='warning';
