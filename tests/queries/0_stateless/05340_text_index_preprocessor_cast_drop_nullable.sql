-- The text index is analyzed through a `CAST` that drops `Nullable`, and the preprocessor is applied to the
-- haystack of the row-level function. The `CAST` stays under the preprocessor, so NULL still throws.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_pre_post;
DROP TABLE IF EXISTS tab_pre;

CREATE TABLE tab_pre_post
(
    id UInt32,
    s Nullable(String),
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(s), postprocessor = lower(s)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

CREATE TABLE tab_pre
(
    id UInt32,
    s Nullable(String),
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(ifNull(s, ''))) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

-- Four granules of two rows. The last granule holds NULL.
INSERT INTO tab_pre_post SELECT number, if(number < 6, concat('Text', toString(number), ' tail'), NULL) FROM numbers(8);
INSERT INTO tab_pre SELECT number, if(number < 6, concat('Text', toString(number), ' tail'), NULL) FROM numbers(8);

SELECT '-- preprocessor and postprocessor';
SELECT id FROM tab_pre_post WHERE NOT hasToken(CAST(s, 'String'), 'text5') ORDER BY id; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT id FROM tab_pre_post WHERE NOT hasAnyTokens(CAST(s, 'String'), 'text5') ORDER BY id; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT id FROM tab_pre_post WHERE NOT hasToken(s::String, 'text5') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT id FROM tab_pre_post WHERE NOT hasToken(CAST(s, 'String'), 'text5') ORDER BY id SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
-- The granule with NULL is skipped.
SELECT id FROM tab_pre_post WHERE hasToken(CAST(s, 'String'), 'TEXT5') ORDER BY id;
SELECT id FROM tab_pre_post WHERE hasAnyTokens(CAST(s, 'String'), 'TEXT1 TEXT5') ORDER BY id;

SELECT '-- preprocessor that maps NULL to a string';
SELECT id FROM tab_pre WHERE NOT hasToken(CAST(s, 'String'), 'text5') ORDER BY id; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT id FROM tab_pre WHERE NOT hasToken(CAST(s, 'String'), 'text5') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT id FROM tab_pre WHERE hasToken(CAST(s, 'String'), 'TEXT5') ORDER BY id;

DROP TABLE tab_pre_post;
DROP TABLE tab_pre;
