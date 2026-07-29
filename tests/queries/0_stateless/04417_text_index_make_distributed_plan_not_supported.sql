-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test: direct read from a text index replaces a text-search function with a synthetic
-- `__text_index_..._has_<hash>` column that exists only in the coordinator's read step. A worker rebuilds
-- its fragment against the table, where that column does not exist, so a shipped read fails at execution
-- with NOT_FOUND_COLUMN_IN_BLOCK. make_distributed_plan rejects such a read at planning time instead.
-- Only a plan that is actually split into fragments ships the read: a single-stage plan is executed
-- locally, so it must keep working.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_text_dist_guard;

CREATE TABLE t_text_dist_guard (k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_text_dist_guard SELECT number, 'uniform' FROM numbers(258);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, query_plan_direct_read_from_text_index = 1;

-- Multi-stage plans ship the read to a worker, so they are rejected.

SELECT '-- WHERE hasToken';
SELECT count() FROM t_text_dist_guard WHERE hasToken(s, 'uniform'); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- PREWHERE hasToken';
SELECT count() FROM t_text_dist_guard PREWHERE hasToken(s, 'uniform'); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- hasAnyTokens';
SELECT count() FROM t_text_dist_guard PREWHERE hasAnyTokens(s, 'uniform') WHERE hasAnyTokens(s, 'uniform'); -- { serverError SUPPORT_IS_DISABLED }

-- The same predicate in PREWHERE and WHERE, which is what the AST fuzzer produced, is rejected too once
-- the plan is split: the read is still shipped.
SELECT '-- hasToken in PREWHERE and WHERE';
SELECT k FROM t_text_dist_guard
PREWHERE (materialize(65537) >= k) AND hasToken(s, 'uniform')
WHERE xor(hasToken(s, 'uniform'), (k >= 65537))
ORDER BY k LIMIT 3; -- { serverError SUPPORT_IS_DISABLED }

-- A single-stage plan is executed locally, so the synthetic column never has to be reconstructed and the
-- query must keep running. This is the exact shape the AST fuzzer produced: the negative LIMIT prunes all
-- parts, so no exchange is created.
SELECT '-- single stage: still runs';
SELECT k FROM t_text_dist_guard
PREWHERE (materialize(65537) >= k) AND hasToken(s, '')
WHERE xor(hasToken(s, ''), (k >= 65537))
LIMIT -2147483649;

-- A read the distributed transform declines to distribute (the table fits in a broadcast) also stays
-- single-stage, so it keeps working and returns the same rows as the non-distributed plan. No ORDER BY:
-- sorting is distributed on its own and would split the plan again.
SELECT '-- broadcast-sized read: still runs';
SELECT k FROM t_text_dist_guard PREWHERE hasToken(s, 'uniform') WHERE k < 2
SETTINGS distributed_plan_max_rows_to_broadcast = 1000000;

-- Without make_distributed_plan the same text index direct reads keep working and return correct results.
SET make_distributed_plan = 0;

SELECT '-- not distributed: results are correct';
SELECT count() FROM t_text_dist_guard WHERE hasToken(s, 'uniform');
SELECT k FROM t_text_dist_guard PREWHERE hasToken(s, 'uniform') WHERE hasToken(s, 'uniform') AND k < 3 ORDER BY k;

DROP TABLE t_text_dist_guard;
