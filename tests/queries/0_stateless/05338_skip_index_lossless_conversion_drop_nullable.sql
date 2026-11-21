-- Skip indexes look through a `CAST` that drops `Nullable` from the indexed column. It does not change a non-NULL
-- value, and it throws on NULL. A query that would throw does not throw if the granules with NULL are skipped.
-- The text index is not read directly instead of such a predicate, otherwise `NOT` would return the NULL row.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s_bf Nullable(String),
    s_lc LowCardinality(Nullable(String)),
    s_text Nullable(String),
    INDEX idx_bf s_bf TYPE bloom_filter GRANULARITY 1,
    INDEX idx_lc s_lc TYPE bloom_filter GRANULARITY 1,
    INDEX idx_text s_text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- One part with four granules of two rows. The last granule holds NULL.
INSERT INTO tab SELECT
    number,
    if(number < 6, concat('bf', toString(number)), NULL),
    if(number < 6, concat('lc', toString(number)), NULL),
    if(number < 6, concat('text', toString(number), ' tail'), NULL)
FROM numbers(8);

SELECT '-- bloom_filter';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE CAST(s_bf, 'String') = 'bf5') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE CAST(s_bf, 'String') = 'bf5' SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT id FROM tab WHERE CAST(s_bf, 'String') IN ('bf1', 'bf5') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT id FROM tab WHERE has(['bf1', 'bf5'], CAST(s_bf, 'String')) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT id FROM tab WHERE CAST(s_lc, 'String') = 'lc5' SETTINGS force_data_skipping_indices = 'idx_lc';
SELECT id FROM tab WHERE CAST(s_lc, 'LowCardinality(String)') IN ('lc1', 'lc5') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_lc';

SELECT '-- text';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(CAST(s_text, 'String'), 'text5')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT id FROM tab WHERE hasToken(CAST(s_text, 'String'), 'text5') SETTINGS force_data_skipping_indices = 'idx_text';

SELECT '-- the granule with NULL is skipped, so the query does not throw';
SELECT id FROM tab WHERE CAST(s_bf, 'String') = 'bf5';
SELECT id FROM tab WHERE CAST(s_bf, 'String') = 'bf5' SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT '-- the text index is not read directly instead of a cast that throws on NULL';
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SELECT countIf(position(explain, '__text_index_') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(CAST(s_text, 'String'), 'text5'));
SELECT id FROM tab WHERE hasToken(CAST(s_text, 'String'), 'text5') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT id FROM tab WHERE NOT hasToken(CAST(s_text, 'String'), 'text5'); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT countIf(position(explain, '__text_index_') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(s_text, 'text5'));

DROP TABLE tab;
