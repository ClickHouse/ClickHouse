-- Direct read from a text index under plan-based parallel replicas. The captured fragment carries
-- the __text_index_* rewrite; a remote replica receives the serialized ReadFromMergeTree together
-- with the text search queries behind the virtual columns and rebuilds the index read tasks against
-- its own copy of the table. Every query is repeated with enable_parallel_replicas = 0; the results
-- must be identical.

DROP TABLE IF EXISTS t_text_pr_plan;

CREATE TABLE t_text_pr_plan (id UInt64, s String, INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_pr_plan SELECT number, 'word' || toString(number) FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
-- Pin the manual mode: otherwise CI's randomized automatic_parallel_replicas_mode can cost-decide
-- against parallel replicas for this small table, so the plan-based split does not engage.
SET automatic_parallel_replicas_mode = 0;
SET query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT '-- exact mode';

SELECT count() FROM t_text_pr_plan WHERE hasToken(s, 'word42');
SELECT count() FROM t_text_pr_plan WHERE hasToken(s, 'word42') SETTINGS enable_parallel_replicas = 0;

SELECT count() FROM t_text_pr_plan WHERE hasAnyTokens(s, ['word42', 'word43', 'nonexistent']);
SELECT count() FROM t_text_pr_plan WHERE hasAnyTokens(s, ['word42', 'word43', 'nonexistent']) SETTINGS enable_parallel_replicas = 0;

SELECT sum(id) FROM t_text_pr_plan WHERE hasAllTokens(s, ['word4242']);
SELECT sum(id) FROM t_text_pr_plan WHERE hasAllTokens(s, ['word4242']) SETTINGS enable_parallel_replicas = 0;

SELECT '-- the same predicate in PREWHERE and WHERE';

-- The fuzzer shape that used to throw the logical error `Column ... already added for reading`
-- when the local fragment of a plan-based parallel-replicas plan was re-optimized.
SELECT count() FROM t_text_pr_plan PREWHERE hasToken(s, 'word42') WHERE hasToken(s, 'word42');
SELECT count() FROM t_text_pr_plan PREWHERE hasToken(s, 'word42') WHERE hasToken(s, 'word42') SETTINGS enable_parallel_replicas = 0;

SELECT '-- the plan splits into a local and a remote parallel-replicas read';

SELECT
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_local_read
FROM (EXPLAIN pretty=0, description=0 SELECT count() FROM t_text_pr_plan WHERE hasToken(s, 'word42'));

-- The join shape from #112723: one join side carries a direct text-index read, and the result is
-- sensitive to that filter ('world bbb' matches the join key but not the token filter), so losing
-- the shipped index read tasks would silently change the result.
SELECT '-- a join side with a direct text-index read (filter-sensitive result)';

DROP TABLE IF EXISTS t_pr_join_l;
DROP TABLE IF EXISTS t_pr_join_r;

CREATE TABLE t_pr_join_r (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pr_join_r VALUES ('hello aaa'), ('world bbb'), ('hello ccc');

CREATE TABLE t_pr_join_l (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pr_join_l VALUES ('hello aaa'), ('world bbb'), ('hello ccc'), ('unmatched');

SELECT count() FROM t_pr_join_l AS l JOIN t_pr_join_r AS r ON l.s = r.s WHERE hasToken(r.s, 'hello');
SELECT count() FROM t_pr_join_l AS l JOIN t_pr_join_r AS r ON l.s = r.s WHERE hasToken(r.s, 'hello')
    SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_pr_join_l;
DROP TABLE t_pr_join_r;

DROP TABLE t_text_pr_plan;
