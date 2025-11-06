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
ORDER BY tuple();

INSERT INTO tab VALUES
  (1, 'Alick'),
  (2, 'clickhouse'),
  (3, 'clickbench'),
  (4, 'Elick'),
  (5, 'Alick');


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

INSERT INTO tab VALUES
  (1, 'Alick'),
  (2, 'clickhouse'),
  (3, 'clickbench'),
  (4, 'Elick'),
  (5, 'Alick');

SELECT tokens(msg, 'sparseGrams') FROM tab;

DROP TABLE tab;
