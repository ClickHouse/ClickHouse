-- The predicate `parallel_replicas_ship_join_predicate` ships stands for the whole join, so its source
-- subquery has to repeat everything the query says about that side of the join: a non-equality conjunct of
-- the `ON`, and - when the join is the entire join tree - a conjunct of the enclosing `WHERE`. Without them
-- the shipped `IN` would list every key of the build side instead of the keys the join can match.

DROP TABLE IF EXISTS sjpsf_probe;
DROP TABLE IF EXISTS sjpsf_dim;

CREATE TABLE sjpsf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO sjpsf_probe SELECT number, number FROM numbers(1000);

CREATE TABLE sjpsf_dim (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO sjpsf_dim SELECT number * 100, number FROM numbers(10);

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

SELECT 'a non-equality ON conjunct is repeated inside the shipped subquery';
-- One `greater` node without the rewrite, two with it: the join keeps its own and the subquery gets a copy.
SELECT countIf(explain LIKE '%function_name: greater%') FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
    JOIN sjpsf_dim AS d ON agg.k = d.k AND d.x > 5
) SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT countIf(explain LIKE '%function_name: greater%') FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
    JOIN sjpsf_dim AS d ON agg.k = d.k AND d.x > 5
) SETTINGS parallel_replicas_ship_join_predicate = 1;

SELECT 'a WHERE conjunct on the source side is repeated too';
SELECT countIf(explain LIKE '%function_name: greater%') FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
    JOIN sjpsf_dim AS d ON agg.k = d.k WHERE d.x > 5
) SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT countIf(explain LIKE '%function_name: greater%') FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
    JOIN sjpsf_dim AS d ON agg.k = d.k WHERE d.x > 5
) SETTINGS parallel_replicas_ship_join_predicate = 1;

SELECT 'a WHERE conjunct on the target side is not';
SELECT countIf(explain LIKE '%function_name: greater%') FROM (
    EXPLAIN QUERY TREE
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
    JOIN sjpsf_dim AS d ON agg.k = d.k WHERE agg.s > 5
) SETTINGS parallel_replicas_ship_join_predicate = 1;

SELECT 'results are identical in every mode';
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
JOIN sjpsf_dim AS d ON agg.k = d.k AND d.x > 5 SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
JOIN sjpsf_dim AS d ON agg.k = d.k AND d.x > 5 SETTINGS parallel_replicas_ship_join_predicate = 1;
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
JOIN sjpsf_dim AS d ON agg.k = d.k AND d.x > 5 SETTINGS parallel_replicas_ship_join_predicate = 2;

SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
JOIN sjpsf_dim AS d ON agg.k = d.k WHERE d.x > 5 SETTINGS parallel_replicas_ship_join_predicate = 0;
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
JOIN sjpsf_dim AS d ON agg.k = d.k WHERE d.x > 5 SETTINGS parallel_replicas_ship_join_predicate = 1;
SELECT sum(agg.s), count() FROM (SELECT k, sum(v) AS s FROM sjpsf_probe GROUP BY k) AS agg
JOIN sjpsf_dim AS d ON agg.k = d.k WHERE d.x > 5 SETTINGS parallel_replicas_ship_join_predicate = 2;

DROP TABLE sjpsf_probe;
DROP TABLE sjpsf_dim;
