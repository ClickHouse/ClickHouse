-- Tags: no-fasttest
-- Coverage for PR #99357: unicode_word tokenizer – hasToken and equality
-- predicates through MergeTreeIndexConditionText, and empty stop_words array.
-- 1. Empty stop_words array: CJK comma is NOT filtered (no defaults applied)
SELECT tokens('你好，世界', 'unicode_word', []);

-- 2. Text index with unicode_word: equality predicate
SET enable_analyzer = 1;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS test_04050_eq;
CREATE TABLE test_04050_eq (key UInt64, str String, INDEX text_idx(str) TYPE text(tokenizer = unicode_word)) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_04050_eq VALUES (1, 'hello world'), (2, 'foo bar');

-- Equality match should use text index and find a granule
EXPLAIN estimate SELECT * FROM test_04050_eq WHERE str = 'hello world';

-- Non-matching equality: text index should skip all granules
EXPLAIN estimate SELECT * FROM test_04050_eq WHERE str = 'baz qux';

DROP TABLE test_04050_eq;

-- 3. Text index with unicode_word: hasToken predicate
DROP TABLE IF EXISTS test_04050_ht;
CREATE TABLE test_04050_ht (key UInt64, str String, INDEX text_idx(str) TYPE text(tokenizer = unicode_word)) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_04050_ht VALUES (1, 'hello错误502需要处理kitty'), (2, 'world正常200运行正常');

-- hasToken with matching ASCII token
EXPLAIN estimate SELECT * FROM test_04050_ht WHERE hasToken(str, 'hello');

-- hasToken with non-matching token
EXPLAIN estimate SELECT * FROM test_04050_ht WHERE hasToken(str, 'missing');

DROP TABLE test_04050_ht;

-- 4. Text index with unicode_word and empty stop_words: CJK comma is indexed as a token
DROP TABLE IF EXISTS test_04050_sw;
CREATE TABLE test_04050_sw (key UInt64, str String, INDEX text_idx(str) TYPE text(tokenizer = unicode_word([]))) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_04050_sw VALUES (1, '你好，世界');

-- With empty stop_words, comma is a token: hasToken must find it (1 granule read)
EXPLAIN estimate SELECT * FROM test_04050_sw WHERE hasToken(str, '，');

-- Non-matching token: index skips the granule
EXPLAIN estimate SELECT * FROM test_04050_sw WHERE hasToken(str, 'missing');

DROP TABLE test_04050_sw;
