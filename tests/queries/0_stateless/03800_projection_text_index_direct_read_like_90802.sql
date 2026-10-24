SET allow_experimental_projection_text_index = 1;
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/90802
-- Text-index direct-read optimization used to throw `NOT_FOUND_COLUMN_IN_BLOCK`
-- when `LIKE '%x%'` appeared both in `SELECT` and `WHERE`. Same coverage for
-- the projection text index direct-read path.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_90802_proj;

CREATE TABLE tab_90802_proj
(
    text String,
    PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab_90802_proj VALUES ('hello suffix world'), ('matching suffix here');

SELECT text LIKE '%suffix%' FROM tab_90802_proj WHERE text LIKE '%suffix%' ORDER BY text;

DROP TABLE tab_90802_proj;
