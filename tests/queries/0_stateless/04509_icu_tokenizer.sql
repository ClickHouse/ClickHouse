-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: the 'icu' tokenizer is only registered when built with ICU

-- The 'icu' tokenizer performs locale-aware Unicode word segmentation (UAX #29 plus dictionary-based
-- breaking, i.e. bundled word dictionaries for scripts without spaces such as Thai and CJK). The locale
-- is passed as the tokenizer-specific argument, e.g. tokens(str, 'icu', 'ja') or, in a text index,
-- tokenizer = icu('ja').

-- { echoOn }
-- English: whitespace/punctuation are dropped, words and numbers are kept
SELECT tokens('The quick, brown fox!', 'icu', 'en');
SELECT tokens('user@example.com sent 3.14 and 1,000', 'icu', 'en');

-- Chinese: dictionary-based segmentation of text without spaces
SELECT tokens('我是一个中国人', 'icu', 'zh');
SELECT tokens('北京欢迎你', 'icu', 'zh');
SELECT tokens('错误503需要处理', 'icu', 'zh');

-- Japanese: mixed kanji / hiragana / katakana
SELECT tokens('日本語の形態素解析', 'icu', 'ja');
SELECT tokens('カタカナとひらがな', 'icu', 'ja');

-- Thai: no spaces between words
SELECT tokens('สวัสดีครับ', 'icu', 'th');

-- Mixed scripts in one string
SELECT tokens('taichi張三丰in the house', 'icu', 'en');

-- edge cases
SELECT tokens('', 'icu', 'en');                    -- empty string

-- locale is mandatory and must not be empty
SELECT tokens('abc', 'icu');                       -- { serverError BAD_ARGUMENTS }
SELECT tokens('abc', 'icu', '');                   -- { serverError BAD_ARGUMENTS }

-- LIKE pattern tokenization is not supported for this tokenizer
SELECT tokensForLikePattern('abc', 'icu', 'en');   -- { serverError BAD_ARGUMENTS }

-- text index integration
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (key UInt64, str String, INDEX text_idx(str) TYPE text(tokenizer = icu('zh'))) ENGINE MergeTree ORDER BY key;
INSERT INTO tab VALUES (1, '北京欢迎你');

-- The needle is tokenized with the same 'icu' tokenizer so it matches the index tokens.
EXPLAIN ESTIMATE SELECT * FROM tab WHERE hasAnyTokens(str, tokens('北京', 'icu', 'zh'));
EXPLAIN ESTIMATE SELECT * FROM tab WHERE hasAnyTokens(str, tokens('上海', 'icu', 'zh'));

DROP TABLE tab;
-- { echoOff }
