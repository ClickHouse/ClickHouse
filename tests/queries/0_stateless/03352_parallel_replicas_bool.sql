CREATE TABLE parallel_replicas_bool (x String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03352', 'r1') ORDER BY tuple();

INSERT INTO parallel_replicas_bool VALUES ('meow');

SELECT materialize(true) FROM parallel_replicas_bool SETTINGS max_parallel_replicas=2, allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', enable_analyzer=1;

SELECT materialize(true) AS x
FROM remote('127.0.0.{1,2}', system.one)
LIMIT 1;

SELECT true AS x
FROM remote('127.0.0.{1,2}', system.one)
LIMIT 1;
