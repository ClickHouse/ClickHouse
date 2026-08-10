-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Repro of issue #109329: the direct-read rewrite replaces text-search functions with the
-- __text_index_* virtual column; a fragment shipped to a worker rebuilds a storage snapshot
-- without it and failed with NOT_FOUND_COLUMN_IN_BLOCK. The serialized ReadFromMergeTree now
-- ships the text search queries behind the virtual columns, and the worker rebuilds the index
-- read tasks against its own copy of the table.

DROP TABLE IF EXISTS t_text_dp;
CREATE TABLE t_text_dp (id UInt64, s String, INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO t_text_dp SELECT number, 'word' || toString(number) FROM numbers(100000);

-- The direct-read setting is pinned to its default (on) so the bug path is provably exercised.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1;

SELECT 'hasAnyTokens over a text index works under make_distributed_plan';
SELECT count() FROM t_text_dp WHERE hasAnyTokens(s, ['word42']);
SELECT count() FROM t_text_dp WHERE hasAnyTokens(s, ['word42']) SETTINGS make_distributed_plan = 0;

SELECT 'the query distributes';
SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM t_text_dp WHERE hasAnyTokens(s, ['word42']))
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

DROP TABLE t_text_dp;
