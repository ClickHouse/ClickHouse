DROP TABLE IF EXISTS checks SYNC;

CREATE TABLE checks
(
    `check_name` LowCardinality(String),
    `check_status` LowCardinality(String),
    `check_start_time` DateTime,
    `test_name` LowCardinality(String),
    `test_status` LowCardinality(String),
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/checks', '{replica}')
ORDER BY check_start_time;

SYSTEM STOP MERGES checks;

INSERT INTO checks SELECT 'asan', if(number % 2, 'success', 'fail'), toDateTime('2024-06-07 00:00:01') + INTERVAL number HOUR, '02221_parallel_replicas_bug', 'FAIL' from numbers(100);
INSERT INTO checks SELECT 'asan', if(number % 2, 'success', 'fail'), toDateTime('2024-06-06 00:00:02') + INTERVAL number HOUR, '02221_parallel_replicas_bug', 'FAIL' from numbers(100);
INSERT INTO checks SELECT 'asan', if(number % 2, 'success', 'fail'), toDateTime('2024-06-05 00:00:03') + INTERVAL number HOUR, '02221_parallel_replicas_bug', 'FAIL' from numbers(100);

SELECT trimBoth(explain)
FROM
(
    EXPLAIN actions=1 SELECT
        check_start_time,
        check_name,
        test_name,
        test_status,
        check_status
    FROM checks
    WHERE 1 AND (test_status != 'SKIPPED') AND (test_status != 'OK') AND (check_status != 'success') AND (test_name ILIKE '%parallel_replicas%')
    ORDER BY
        check_start_time DESC,
        check_name ASC,
        test_name ASC
    SETTINGS query_plan_read_in_order = 1, optimize_read_in_order = 1, max_parallel_replicas = 1
)
WHERE explain LIKE '%InReverseOrder%';

SELECT check_start_time, check_name, test_name, test_status, check_status
FROM checks
WHERE 1
    AND test_status != 'SKIPPED'
    AND test_status != 'OK'
    AND check_status != 'success'
    AND test_name ilike '%parallel_replicas%'
ORDER BY check_start_time desc, check_name, test_name
SETTINGS query_plan_read_in_order = 1, optimize_read_in_order = 1, allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

DROP TABLE checks SYNC;
