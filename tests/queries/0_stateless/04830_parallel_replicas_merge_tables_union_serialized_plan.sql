-- A query whose first stage contains a `UNION ALL` over `Merge` sources (directly or through a
-- view) must return every branch's rows exactly once with parallel replicas, with and without
-- the serialized query plan. Each branch of a direct `UNION ALL` is offloaded as its own
-- fragment with its own reading coordinator, and a view ships unexpanded (the replicas re-plan
-- its inner query); a fragment must coordinate every read the replicas would otherwise perform
-- in full on top of the coordinated result, duplicating rows.
-- https://github.com/ClickHouse/ClickHouse/issues/67770

DROP TABLE IF EXISTS t_pr_union_a_1;
DROP TABLE IF EXISTS t_pr_union_a_2;
DROP TABLE IF EXISTS t_pr_union_b_1;
DROP TABLE IF EXISTS t_pr_union_v_plain;
DROP TABLE IF EXISTS t_pr_union_v_merge;

CREATE TABLE t_pr_union_a_1 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pr_union_a_2 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pr_union_b_1 (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_pr_union_a_1 SELECT number FROM numbers(500);
INSERT INTO t_pr_union_a_2 SELECT number + 500 FROM numbers(500);
INSERT INTO t_pr_union_b_1 SELECT number FROM numbers(500);

CREATE VIEW t_pr_union_v_plain AS SELECT k FROM t_pr_union_a_1 UNION ALL SELECT k FROM t_pr_union_a_2;
CREATE VIEW t_pr_union_v_merge AS SELECT k FROM merge(currentDatabase(), '^t_pr_union_a_[12]$') UNION ALL SELECT k FROM t_pr_union_b_1;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET parallel_replicas_allow_view_over_mergetree = 1;
SET automatic_parallel_replicas_mode = 0;

SELECT '-- merge() UNION ALL merge()';
SELECT count(), sum(k) FROM (SELECT k FROM merge(currentDatabase(), '^t_pr_union_a_[12]$') UNION ALL SELECT k FROM merge(currentDatabase(), '^t_pr_union_b_[1]$')) SETTINGS serialize_query_plan = 0;
SELECT count(), sum(k) FROM (SELECT k FROM merge(currentDatabase(), '^t_pr_union_a_[12]$') UNION ALL SELECT k FROM merge(currentDatabase(), '^t_pr_union_b_[1]$')) SETTINGS serialize_query_plan = 1;

SELECT '-- merge() UNION ALL a MergeTree table';
SELECT count(), sum(k) FROM (SELECT k FROM merge(currentDatabase(), '^t_pr_union_a_[12]$') UNION ALL SELECT k FROM t_pr_union_b_1) SETTINGS serialize_query_plan = 0;
SELECT count(), sum(k) FROM (SELECT k FROM merge(currentDatabase(), '^t_pr_union_a_[12]$') UNION ALL SELECT k FROM t_pr_union_b_1) SETTINGS serialize_query_plan = 1;

SELECT '-- view over UNION ALL of MergeTree tables';
SELECT count(), sum(k) FROM t_pr_union_v_plain SETTINGS serialize_query_plan = 0;
SELECT count(), sum(k) FROM t_pr_union_v_plain SETTINGS serialize_query_plan = 1;

SELECT '-- view over merge() UNION ALL a MergeTree table';
SELECT count(), sum(k) FROM t_pr_union_v_merge SETTINGS serialize_query_plan = 0;
SELECT count(), sum(k) FROM t_pr_union_v_merge SETTINGS serialize_query_plan = 1;

DROP TABLE t_pr_union_v_plain;
DROP TABLE t_pr_union_v_merge;
DROP TABLE t_pr_union_a_1;
DROP TABLE t_pr_union_a_2;
DROP TABLE t_pr_union_b_1;
