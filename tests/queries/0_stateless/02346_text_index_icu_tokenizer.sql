-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: the 'icu' tokenizer uses ICU

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

-- Invalid icu tokenizer arguments must be rejected gracefully (no crash).
SELECT hasAnyTokens('a b', 'b', 'icu'); -- { serverError BAD_ARGUMENTS }
SELECT hasAnyTokens('a b', 'b', 'icu('''')'); -- { serverError BAD_ARGUMENTS }
SELECT hasAnyTokens('a b', 'b', materialize('icu(''ja'')')); -- { serverError ILLEGAL_COLUMN }
-- Like ngrams/splitByString, has*Tokens takes no separate tokenizer-parameter argument.
SELECT hasAnyTokens('a b', 'b', 'icu(''ja'')', 'ja'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_noindex;

-- Build a text index using the 'icu' tokenizer with a Japanese locale.
CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = icu('ja')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

-- Same data, but without any index (forces a brute-force scan).
CREATE TABLE tab_noindex (id UInt32, doc String) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, '私は日本語を勉強しています'),
    (2, 'コンピュータのプログラミング'),
    (3, 'ClickHouseは高速なデータベースです'),
    (4, '東京と大阪は日本の都市です');
INSERT INTO tab_noindex SELECT * FROM tab;

-- has*Tokens infers the tokenizer from the text index, so no tokenizer argument is needed.

-- { echoOn }
-- String needle: tokenized with the index tokenizer (icu). A Latin token embedded in Japanese text.
SELECT id FROM tab WHERE hasAnyTokens(doc, 'ClickHouse') ORDER BY id;
-- String needle: a single Japanese dictionary word.
SELECT id FROM tab WHERE hasAnyTokens(doc, '日本語') ORDER BY id;
-- String needle that icu splits into several words (コンピュータ / の / プログラミング). splitByNonAlpha would
-- keep it as one token and match nothing; matching only row 2 proves the index tokenizer (icu) is used.
SELECT id FROM tab WHERE hasAllTokens(doc, 'コンピュータのプログラミング') ORDER BY id;
-- Array needle: each element is treated as a literal token (no tokenization).
SELECT id FROM tab WHERE hasAnyTokens(doc, ['日本語', '東京']) ORDER BY id;
-- hasAllTokens: all needle tokens must be present in the same row.
SELECT id FROM tab WHERE hasAllTokens(doc, ['日本', '都市']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(doc, ['日本語', '東京']) ORDER BY id;
-- A token that is not present in any row.
SELECT id FROM tab WHERE hasAnyTokens(doc, '存在しない') ORDER BY id;

-- Without an index and without a tokenizer argument, the brute-force scan uses splitByNonAlpha, which
-- cannot segment Japanese: a run with no ASCII separators becomes one token, so searching for an
-- individual Japanese word that is only a part of such a run finds nothing.
SELECT id FROM tab_noindex WHERE hasAnyTokens(doc, '日本語') ORDER BY id;
SELECT id FROM tab_noindex WHERE hasAnyTokens(doc, '東京') ORDER BY id;
SELECT id FROM tab_noindex WHERE hasAllTokens(doc, ['日本', '都市']) ORDER BY id;

-- Specifying the icu tokenizer explicitly (embedded in the tokenizer string, like ngrams/splitByString)
-- makes the brute-force scan segment Japanese correctly, giving exactly the same results as the indexed
-- column (which infers the icu tokenizer). Each pair below (explicit brute-force vs inferred index) must match.
SELECT id FROM tab_noindex WHERE hasAnyTokens(doc, '日本語', 'icu(''ja'')') ORDER BY id;
SELECT id FROM tab         WHERE hasAnyTokens(doc, '日本語') ORDER BY id;

SELECT id FROM tab_noindex WHERE hasAnyTokens(doc, '東京', 'icu(''ja'')') ORDER BY id;
SELECT id FROM tab         WHERE hasAnyTokens(doc, '東京') ORDER BY id;

SELECT id FROM tab_noindex WHERE hasAllTokens(doc, 'コンピュータのプログラミング', 'icu(''ja'')') ORDER BY id;
SELECT id FROM tab         WHERE hasAllTokens(doc, 'コンピュータのプログラミング') ORDER BY id;

SELECT id FROM tab_noindex WHERE hasAllTokens(doc, ['日本', '都市'], 'icu(''ja'')') ORDER BY id;
SELECT id FROM tab         WHERE hasAllTokens(doc, ['日本', '都市']) ORDER BY id;

-- hasPhrase needs an ordered token stream, which the icu tokenizer provides. The tokenizer is inferred
-- from the index, so no tokenizer argument is needed; the explicit form must also be accepted (not rejected).
SELECT id FROM tab WHERE hasPhrase(doc, '日本語') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(doc, '日本語を勉強') ORDER BY id;
-- The same tokens in a different order do not form the phrase.
SELECT id FROM tab WHERE hasPhrase(doc, '勉強を日本語') ORDER BY id;
-- Explicit tokenizer argument must be accepted rather than rejected with BAD_ARGUMENTS.
SELECT id FROM tab WHERE hasPhrase(doc, '日本語', 'icu(''ja'')') ORDER BY id;
-- { echoOff }

DROP TABLE tab;
DROP TABLE tab_noindex;
