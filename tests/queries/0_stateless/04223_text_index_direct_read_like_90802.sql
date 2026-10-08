-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/90802
-- Text-index direct-read optimization used to throw `NOT_FOUND_COLUMN_IN_BLOCK`
-- when `LIKE '%x%'` appeared both in `SELECT` and `WHERE`.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab_90802;

CREATE TABLE tab_90802
(
    text String,
    INDEX text_idx text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab_90802 VALUES ('hello suffix world'), ('matching suffix here');

SELECT text LIKE '%suffix%' FROM tab_90802 WHERE text LIKE '%suffix%' ORDER BY text;

DROP TABLE tab_90802;
