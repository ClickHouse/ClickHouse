SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';
SET query_plan_optimize_join_order_algorithm='greedy';
SET query_plan_optimize_join_order_limit=1;
SET query_plan_join_swap_table=0;

-- Check result without runtime filters
SET enable_join_runtime_filters=0;

SELECT id, value
FROM
  (SELECT materialize(toLowCardinality(toNullable(0))) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id);

-- And with runtime filters
SET enable_join_runtime_filters=1;

SELECT id, value
FROM
  (SELECT materialize(toLowCardinality(toNullable(0))) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id);

SELECT id, value
FROM
  (SELECT toLowCardinality(toNullable(0)) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id);

SELECT id, value
FROM
  (SELECT toNullable(0) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id);

SELECT id, value
FROM
  (SELECT toLowCardinality(toNullable(0)) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(toNullable(0)) AS id) AS d
USING (id);

SELECT id, value
FROM
  (SELECT toLowCardinality(toNullable(0)) AS id, 1 AS value) AS a
  JOIN (SELECT materialize(toLowCardinality(toNullable(0))) AS id) AS d
USING (id);
