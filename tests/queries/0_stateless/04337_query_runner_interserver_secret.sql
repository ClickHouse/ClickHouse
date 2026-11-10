-- Tags: no-fasttest

CREATE TABLE runner_secret (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_cluster_interserver_secret', mode = 'synchronous';
CREATE TABLE runner_secret_dst (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO runner_secret SELECT 'INSERT INTO ' || currentDatabase() || '.runner_secret_dst VALUES (1)';
SELECT * FROM runner_secret_dst;
