-- Test for issue #100879: hasAllTokens/hasAnyTokens give wrong results
-- with query_plan_merge_expressions = 0.
-- When merge_expressions is disabled, an ExpressionStep sits between
-- ReadFromMergeTree and FilterStep. The text index preprocessing that
-- adds the tokenizer argument to hasAllTokens/hasAnyTokens must still
-- be applied in that case.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
  id UInt64,
  msg String,
  INDEX id_msg msg TYPE text(tokenizer = sparseGrams)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO tab SELECT number, 'alick' FROM numbers(1024);
INSERT INTO tab SELECT number, 'clickhouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'clickbench' FROM numbers(1024);
INSERT INTO tab SELECT number, 'blick' FROM numbers(1024);

SET query_plan_direct_read_from_text_index = 0;

SET query_plan_merge_expressions = 1;
SELECT count() FROM tab WHERE hasAllTokens(msg, sparseGrams('click'));
SELECT count() FROM tab WHERE hasAnyTokens(msg, sparseGrams('click'));

SET query_plan_merge_expressions = 0;
SELECT count() FROM tab WHERE hasAllTokens(msg, sparseGrams('click'));
SELECT count() FROM tab WHERE hasAnyTokens(msg, sparseGrams('click'));

-- Same with direct text-index read mode: the DAG composition path is shared,
-- so query_plan_merge_expressions = 0 must also work with query_plan_direct_read_from_text_index = 1.
SET query_plan_direct_read_from_text_index = 1;

SET query_plan_merge_expressions = 1;
SELECT count() FROM tab WHERE hasAllTokens(msg, sparseGrams('click'));
SELECT count() FROM tab WHERE hasAnyTokens(msg, sparseGrams('click'));

SET query_plan_merge_expressions = 0;
SELECT count() FROM tab WHERE hasAllTokens(msg, sparseGrams('click'));
SELECT count() FROM tab WHERE hasAnyTokens(msg, sparseGrams('click'));

DROP TABLE tab;
