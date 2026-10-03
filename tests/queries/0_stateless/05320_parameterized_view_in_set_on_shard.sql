-- Tags: shard

-- An `IN` over a parameterized view, called unqualified, inside an expression that the shards
-- compute and return to the initiator: an aggregate argument, a `GROUP BY` key, a `SELECT` item.

DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS data;
DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS pv;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src VALUES (2);
CREATE VIEW pv AS SELECT x FROM src WHERE x >= {p:UInt64};
CREATE VIEW v AS SELECT x FROM pv(p = 0);

CREATE TABLE data (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO data SELECT number FROM numbers(4);
CREATE TABLE dist AS data ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data);

-- { echoOn }
SELECT countIf(number IN (SELECT x FROM pv(p = 0))) FROM dist;
SELECT countIf(number NOT IN (SELECT x FROM pv(p = 0))) FROM dist;
SELECT number IN (SELECT x FROM pv(p = 0)) AS k, count() FROM dist GROUP BY k ORDER BY k;
SELECT number, number IN (SELECT x FROM pv(p = 0)) FROM dist ORDER BY number;
SELECT countIf(number IN (SELECT x FROM pv(p = 0))) FROM remote('127.0.0.{1,2}', currentDatabase(), data);
SELECT countIf(number IN (SELECT x FROM pv(p = 0))), countIf(number IN (SELECT x FROM pv(p = 3))) FROM dist;
SELECT countIf(number IN (SELECT x FROM v)) FROM dist SETTINGS analyzer_inline_views = 1;
SELECT countIf(number IN (SELECT x FROM pv(p = 0))) FROM data SETTINGS enable_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;
-- { echoOff }

DROP TABLE dist;
DROP TABLE data;
DROP VIEW v;
DROP VIEW pv;
DROP TABLE src;
