-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test: a direct read from a text index adds ephemeral virtual columns (and a separate
-- index-read step) that a distributed worker fragment cannot reproduce. make_distributed_plan must
-- reject such a read at planning time instead of crashing later. Previously the single-stage path
-- re-ran the non-idempotent text index optimization and aborted with the LOGICAL_ERROR
-- "Column __text_index_idx_... already added for reading" (server abort in debug/sanitizer builds);
-- the multi-stage path failed with NOT_FOUND_COLUMN_IN_BLOCK at execution.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_text_dist_guard;

CREATE TABLE t_text_dist_guard (k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_text_dist_guard SELECT number, 'uniform' FROM numbers(258);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, query_plan_direct_read_from_text_index = 1;

-- The exact shape the AST fuzzer produced: the same hasToken predicate in PREWHERE and WHERE keeps
-- the plan single-stage, which used to re-run the text index optimization and abort the server.
SELECT '-- fuzzer shape: hasToken in PREWHERE and WHERE';
SELECT k FROM t_text_dist_guard
PREWHERE (materialize(65537) >= k) AND hasToken(s, '')
WHERE xor(hasToken(s, ''), (k >= 65537))
LIMIT -2147483649; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- WHERE hasToken';
SELECT count() FROM t_text_dist_guard WHERE hasToken(s, 'uniform'); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- PREWHERE hasToken';
SELECT count() FROM t_text_dist_guard PREWHERE hasToken(s, 'uniform'); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- hasAnyTokens';
SELECT count() FROM t_text_dist_guard PREWHERE hasAnyTokens(s, 'uniform') WHERE hasAnyTokens(s, 'uniform'); -- { serverError SUPPORT_IS_DISABLED }

-- Without make_distributed_plan the same text index direct reads keep working and return correct results.
SET make_distributed_plan = 0;

SELECT '-- not distributed: results are correct';
SELECT count() FROM t_text_dist_guard WHERE hasToken(s, 'uniform');
SELECT k FROM t_text_dist_guard PREWHERE hasToken(s, 'uniform') WHERE hasToken(s, 'uniform') AND k < 3 ORDER BY k;

DROP TABLE t_text_dist_guard;
