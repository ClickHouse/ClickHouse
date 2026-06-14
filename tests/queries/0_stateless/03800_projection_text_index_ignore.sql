SET allow_experimental_projection_text_index = 1;
-- Verify that `ignore_data_skipping_indices = '<projection_name>'` disables
-- a projection-backed text index for that projection.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    k UInt64,
    s String,
    PROJECTION p_text INDEX s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;

-- 'rare' appears only in row 100, so a working text index drops 31/32 granules.
INSERT INTO tab SELECT number, if(number = 100, 'rareToken', format('foo {} bar', number)) FROM numbers(8192);

SELECT 'Without ignore: projection text index drops granules';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(s, 'rareToken')
) WHERE explain ILIKE '%p_text%' OR explain ILIKE '%Granules:%';

SELECT 'With ignore = p_text: index is not listed and no granules dropped';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(s, 'rareToken')
    SETTINGS ignore_data_skipping_indices = 'p_text'
) WHERE explain ILIKE '%p_text%' OR explain ILIKE '%Granules:%';

SELECT 'Result is identical with and without ignore';
SELECT count() FROM tab WHERE hasToken(s, 'rareToken');
SELECT count() FROM tab WHERE hasToken(s, 'rareToken') SETTINGS ignore_data_skipping_indices = 'p_text';

SELECT 'Ignoring an unrelated name does not affect the projection text index';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasToken(s, 'rareToken')
    SETTINGS ignore_data_skipping_indices = 'something_else'
) WHERE explain ILIKE '%p_text%';

DROP TABLE tab;
