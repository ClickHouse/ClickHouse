-- Direct read from a text index replaces a text-search function with a synthetic
-- `__text_index_..._has_<hash>` virtual column registered on the `ReadFromMergeTree` step.
-- That per-step state does not survive capturing the read into a plan-based parallel-replicas
-- fragment, so re-optimizing the local fragment used to throw the logical error
-- `Column ... already added for reading` when the same predicate appeared in both PREWHERE
-- and WHERE (found by AST fuzzer on a mutation of 04338_text_index_codec_setting_change_merge).
-- Such a plan must stay local.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE IF EXISTS t_text_index_pr_plan_based;

CREATE TABLE t_text_index_pr_plan_based (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_pr_plan_based SELECT 'hello world ' || toString(number) FROM numbers(1000);
INSERT INTO t_text_index_pr_plan_based SELECT 'foo bar ' || toString(number) FROM numbers(1000);

-- The same predicate in PREWHERE and WHERE used to throw `Column ... already added for reading`.
SELECT count() FROM t_text_index_pr_plan_based PREWHERE hasToken(s, 'hello') WHERE hasToken(s, 'hello');
SELECT count() FROM t_text_index_pr_plan_based WHERE hasToken(s, 'hello');

-- The plan with a direct read from a text index must not be split for parallel replicas.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN pretty=0, description=0 SELECT count() FROM t_text_index_pr_plan_based PREWHERE hasToken(s, 'hello') WHERE hasToken(s, 'hello'));

-- A read without a text-index direct read is still split.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN pretty=0, description=0 SELECT count() FROM t_text_index_pr_plan_based WHERE s != '');

DROP TABLE t_text_index_pr_plan_based;
