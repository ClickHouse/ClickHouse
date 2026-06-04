-- Tags: no-fasttest
-- no-fasttest: the chinese tokenizer uses Jieba, which is not available in the fast test build

-- Tests that the `chinese` tokenizer is usable in a `MergeTree` text index (not only via the
-- `tokens` function). The index path has separate ingestion and query-side code, so we exercise
-- it end-to-end: build the index, then answer `hasToken` / `hasAllTokens` queries through it and
-- assert the index is actually applicable using `force_data_skipping_indices`.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = 'chinese') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO tab VALUES (1, '他来到了网易杭研大厦'), (2, '我来自北京邮电大学'), (3, '南京市长江大桥');

SELECT '-- tokens produced by the coarse_grained chinese tokenizer';
SELECT tokens('我来自北京邮电大学', 'chinese');

SELECT '-- hasToken matches the segmented tokens';
SELECT id FROM tab WHERE hasToken(doc, '北京邮电大学') ORDER BY id;
SELECT id FROM tab WHERE hasToken(doc, '大厦') ORDER BY id;
SELECT count() FROM tab WHERE hasToken(doc, '不存在');

SELECT '-- the chinese text index is applicable for the predicate';
SELECT id FROM tab WHERE hasToken(doc, '北京邮电大学') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- hasAllTokens through the index';
SELECT id FROM tab WHERE hasAllTokens(doc, ['北京邮电大学']) ORDER BY id;

DROP TABLE tab;

-- The fine_grained granularity is also accepted as a text index tokenizer argument.
DROP TABLE IF EXISTS tab_fine;

CREATE TABLE tab_fine
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = chinese('fine_grained')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO tab_fine VALUES (1, '我来自北京邮电大学'), (2, '南京市长江大桥');

SELECT '-- fine_grained emits overlapping sub-tokens such as 大学';
SELECT id FROM tab_fine WHERE hasToken(doc, '大学') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_fine;
