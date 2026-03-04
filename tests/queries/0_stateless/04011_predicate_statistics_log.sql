-- Tags: no-fasttest
-- Verify that system.predicate_statistics_log collects per-predicate selectivity data
-- requires server setting predicate_statistics_sample_rate = 1 (in test config)

DROP TABLE IF EXISTS test_pred_stats;

CREATE TABLE test_pred_stats (id UInt64, status String, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_pred_stats SELECT number, if(number % 10 = 0, 'active', 'inactive'), rand() FROM numbers(100000);

SELECT count() FROM test_pred_stats WHERE status = 'active' FORMAT Null;
SELECT count() FROM test_pred_stats WHERE id > 50000 FORMAT Null;
SELECT count() FROM test_pred_stats WHERE id IN (1, 2, 3) FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT
    column_name,
    predicate_class,
    function_name,
    sum(input_rows) > 0 AS has_input,
    sum(passed_rows) > 0 AS has_output
FROM system.predicate_statistics_log
WHERE table = 'test_pred_stats' AND currentDatabase() = database
GROUP BY column_name, predicate_class, function_name
ORDER BY column_name, predicate_class, function_name;

DROP TABLE test_pred_stats;
