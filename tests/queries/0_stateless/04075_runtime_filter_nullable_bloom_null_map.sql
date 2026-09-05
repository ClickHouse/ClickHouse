SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_limit = 1;
SET max_threads = 1;
SET max_block_size = 10;
SET join_runtime_filter_min_probe_rows = 0;

CREATE TABLE rf_left(k Nullable(Int32), payload UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE rf_right(k Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO rf_left SELECT if(number % 5 = 0, NULL, toInt32(number % 10)), 1 FROM numbers(200);
INSERT INTO rf_right VALUES (1), (2), (NULL);

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM rf_left l INNER JOIN rf_right r USING (k)
    SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 1
)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT count()
FROM rf_left l INNER JOIN rf_right r USING (k)
SETTINGS enable_join_runtime_filters = 0, join_runtime_filter_exact_values_limit = 1;

SELECT count()
FROM rf_left l INNER JOIN rf_right r USING (k)
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 1;

SELECT count()
FROM rf_left l INNER JOIN rf_right r USING (k)
SETTINGS
    enable_join_runtime_filters = 1,
    join_runtime_filter_exact_values_limit = 1,
    log_comment = 'RF_NULLABLE_BLOOM_04075';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFilterRowsChecked'] > 0
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND log_comment = 'RF_NULLABLE_BLOOM_04075'
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

TRUNCATE TABLE rf_right;
INSERT INTO rf_right VALUES (NULL);

SELECT count()
FROM rf_left l INNER JOIN rf_right r USING (k)
SETTINGS
    enable_join_runtime_filters = 1,
    join_runtime_filter_exact_values_limit = 100,
    log_comment = 'RF_NULL_EXACT_04075';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFilterRowsPassed'] = 0
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND log_comment = 'RF_NULL_EXACT_04075'
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE rf_left;
DROP TABLE rf_right;
