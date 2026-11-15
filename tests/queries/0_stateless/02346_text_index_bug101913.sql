-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101913
-- Full-text index direct-read threw LOGICAL_ERROR when multiple filter conditions
-- (e.g. hasAllTokens AND LIKE) were used on a Merge table with query_plan_direct_read_from_text_index=1.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_merged;

CREATE TABLE tab
(
    `id` UInt64,
    `str` String,
    INDEX idx_text str TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_merged AS tab ENGINE = Merge(currentDatabase(), 'tab');

INSERT INTO tab (id, str) VALUES (1, 'error'), (2, 'something (par)'), (3, 'nothing');

SELECT str FROM tab_merged WHERE
  hasAllTokens(str, ['par'])
  AND str LIKE '%(par)%'
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT str FROM tab_merged PREWHERE
  hasAllTokens(str, ['par'])
  AND str LIKE '%(par)%'
SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE tab_merged;
DROP TABLE tab;
