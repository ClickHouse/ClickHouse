-- `parallel_replicas_ship_join_predicate` injects an INNER JOIN's semi-join predicate into the subquery
-- on the other side, so a parallel-replicas fragment - shipped as query text, which cannot carry a join
-- runtime filter - filters before aggregating. The join stays put, so results must not change.

DROP TABLE IF EXISTS sjp_probe;
DROP TABLE IF EXISTS sjp_dim;
DROP TABLE IF EXISTS sjp_dim_dup;

CREATE TABLE sjp_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO sjp_probe SELECT number, number FROM numbers(10000);

CREATE TABLE sjp_dim (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO sjp_dim SELECT number * 1000 FROM numbers(10);

-- Four rows per key: an INNER JOIN multiplies matching left rows, an `IN` would not.
CREATE TABLE sjp_dim_dup (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO sjp_dim_dup SELECT number % 10 * 1000 FROM numbers(40);

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

SELECT 'predicate reaches the WHERE of the shipped subquery';
SELECT countIf(explain LIKE '%function_name: in,%') AS in_nodes, countIf(explain LIKE '%WHERE%') AS where_clauses
FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim AS d ON agg.k = d.k
) SETTINGS parallel_replicas_ship_join_predicate = 1;

SELECT countIf(explain LIKE '%function_name: globalIn,%') AS global_in_nodes
FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim AS d ON agg.k = d.k
) SETTINGS parallel_replicas_ship_join_predicate = 2;

SELECT 'results are identical in every mode';
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 1;
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 2;

SELECT 'duplicate build keys keep their multiplicity';
SELECT count(), max(agg.s), uniqExact(agg.s) FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim_dup AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT count(), max(agg.s), uniqExact(agg.s) FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim_dup AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 1;
SELECT count(), max(agg.s), uniqExact(agg.s) FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg JOIN sjp_dim_dup AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 2;

SELECT 'a LEFT JOIN keeps its unmatched rows';
SELECT count() FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg LEFT JOIN sjp_dim AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT count() FROM (SELECT k, sum(v) AS s FROM sjp_probe GROUP BY k) AS agg LEFT JOIN sjp_dim AS d ON agg.k = d.k
SETTINGS parallel_replicas_ship_join_predicate = 1;

DROP TABLE sjp_probe;
DROP TABLE sjp_dim;
DROP TABLE sjp_dim_dup;
