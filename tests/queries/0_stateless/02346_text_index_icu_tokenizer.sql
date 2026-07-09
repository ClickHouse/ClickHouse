-- Tags: no-fasttest
-- no-fasttest: the 'icu' tokenizer uses ICU

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

-- Build a text index using the 'icu' tokenizer with a Japanese locale.
CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = icu('ja')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, '私は日本語を勉強しています'),
    (2, 'コンピュータのプログラミング'),
    (3, 'ClickHouseは高速なデータベースです'),
    (4, '東京と大阪は日本の都市です');

-- The has*Tokens functions must use the same tokenizer as the index, passed via function-call syntax.

-- { echoOn }
-- A Latin token embedded in Japanese text (deterministic segmentation).
SELECT id FROM tab WHERE hasAnyTokens(doc, 'ClickHouse', 'icu(''ja'')') ORDER BY id;
-- Single Japanese dictionary words.
SELECT id FROM tab WHERE hasAnyTokens(doc, '日本語', 'icu(''ja'')') ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(doc, '東京', 'icu(''ja'')') ORDER BY id;
-- hasAnyTokens with multiple needles: match if any token is present.
SELECT id FROM tab WHERE hasAnyTokens(doc, ['日本語', '東京'], 'icu(''ja'')') ORDER BY id;
-- hasAllTokens: all needle tokens must be present in the same row.
SELECT id FROM tab WHERE hasAllTokens(doc, ['日本', '都市'], 'icu(''ja'')') ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(doc, ['日本語', '東京'], 'icu(''ja'')') ORDER BY id;
-- A token that is not present in any row.
SELECT id FROM tab WHERE hasAnyTokens(doc, '存在しない', 'icu(''ja'')') ORDER BY id;
-- { echoOff }

DROP TABLE tab;
