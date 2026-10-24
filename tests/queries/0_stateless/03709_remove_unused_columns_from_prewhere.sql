SET enable_parallel_replicas = 0;
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    `account_id` Int64,
    `country` String,
    `source_type` String,
    `day` DateTime
)
ENGINE = MergeTree
ORDER BY (day, account_id);

INSERT INTO test SELECT * FROM generateRandom('account_id Int64, country String, source_type String, day DateTime', 42) LIMIT 100;

SET enable_analyzer = 1;

EXPLAIN PLAN header = 1
SELECT
    account_id AS accountId,
    country AS country
FROM test
WHERE (day >= '2025-07-01') AND (day <= '2025-07-31') AND (source_type IN ('1')) AND (account_id < 900000000)
SETTINGS query_plan_remove_unused_columns = 0;

SELECT '-----';

EXPLAIN PLAN header = 1
SELECT
    account_id AS accountId,
    country AS country
FROM test
WHERE (day >= '2025-07-01') AND (day <= '2025-07-31') AND (source_type IN ('1')) AND (account_id < 900000000)
SETTINGS query_plan_remove_unused_columns = 1;
