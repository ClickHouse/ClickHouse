-- Tags: distributed, zookeeper

CREATE TABLE test ON CLUSTER test_shard_localhost
(
    val UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/03631_common_table_expression/{database}/{table}', 'r1')
ORDER BY val;

INSERT INTO test WITH cte AS (SELECT 314) SELECT * FROM cte;

WITH cte AS (SELECT 2718) INSERT INTO test SELECT * FROM cte;

SELECT * FROM test ORDER BY val;

DROP TABLE test ON CLUSTER test_shard_localhost;
