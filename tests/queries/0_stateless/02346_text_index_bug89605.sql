-- Test for issue #89605

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 0;
SET max_threads = 2; -- make sure it's running multi-threaded

SELECT 'sparseGrams tokenizer is provided to hasAnyTokens and hasAllTokens functions';

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


SELECT count() FROM tab WHERE hasAllTokens(msg, sparseGrams('click'));

SELECT count() FROM tab WHERE hasAnyTokens(msg, sparseGrams('click'));

DROP TABLE tab;

SELECT 'sparseGrams tokenizer is provided to the tokens function';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
  id UInt64,
  msg String
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO tab SELECT number, 'alick' FROM numbers(1024);
INSERT INTO tab SELECT number, 'clickhouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'clickbench' FROM numbers(1024);
INSERT INTO tab SELECT number, 'blick' FROM numbers(1024);

SELECT sum(length(tokens(msg, 'sparseGrams'))) FROM tab;

DROP TABLE tab;
