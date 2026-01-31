SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';
SET query_plan_optimize_join_order_algorithm='greedy';
SET query_plan_optimize_join_order_limit=1;
SET query_plan_join_swap_table=0;
SET allow_suspicious_low_cardinality_types=1;
SET allow_experimental_dynamic_type=1;
SET allow_dynamic_type_in_join_keys=1;


CREATE TABLE t0 (c0 Dynamic) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 (c0 LowCardinality(Nullable(Int))) ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO TABLE t0 (c0) VALUES (1::LowCardinality(Int));
INSERT INTO TABLE t1 (c0) VALUES (1);

-- Check result without runtime filters
SET enable_join_runtime_filters=0;

SELECT id, value
FROM
  (SELECT materialize(toLowCardinality(toNullable(0))) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id);

SELECT 1 FROM t0 JOIN t1 USING c0;

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

SELECT 1 FROM t0 JOIN t1 USING c0;
