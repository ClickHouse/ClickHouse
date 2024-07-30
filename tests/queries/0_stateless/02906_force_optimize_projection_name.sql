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

INSERT INTO test SELECT number, 'test' FROM numbers(1, 100);

SELECT name FROM test GROUP BY name SETTINGS force_optimize_projection_name='projection_name';

SELECT name FROM test GROUP BY name SETTINGS force_optimize_projection_name='non_existing_projection'; -- { serverError 117 }

SELECT name FROM test SETTINGS force_optimize_projection_name='projection_name'; -- { serverError 117 }
