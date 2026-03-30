DROP TABLE IF EXISTS test;

CREATE TABLE test
(
   `id` UInt64,
   `name` String,
   PROJECTION projection_name
   (
       SELECT sum(id) GROUP BY id, name
   )
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity_bytes = 10000;

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

INSERT INTO test SELECT number, 'test' FROM numbers(1, 100);

SELECT name FROM test GROUP BY name SETTINGS force_optimize_projection_name='projection_name';

SELECT name FROM test GROUP BY name SETTINGS force_optimize_projection_name='non_existing_projection'; -- { serverError INCORRECT_DATA }

SELECT name FROM test SETTINGS force_optimize_projection_name='projection_name'; -- { serverError INCORRECT_DATA }

INSERT INTO test SELECT number, 'test' FROM numbers(1, 100) SETTINGS force_optimize_projection_name='projection_name';
SELECT 1 SETTINGS force_optimize_projection_name='projection_name';

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%SELECT name FROM test%'
    AND Settings['force_optimize_projection_name'] = 'projection_name'
    AND type = 'ExceptionBeforeStart';

DROP TABLE test;
