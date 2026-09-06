-- Tests that text, tokenbf_v1 and ngrambf_v1 skip indexes prune an IN with transform_null_in = 1,
-- where the predicate arrives as nullIn/globalNullIn, and that a set element the index cannot
-- compare byte for byte is refused instead.

SET enable_full_text_index = 1;
SET transform_null_in = 1;

DROP TABLE IF EXISTS tab;

-- text

CREATE TABLE tab (s Nullable(String), INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(4);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';

-- A NULL element matches the column's NULL rows, which the index cannot express.
SELECT count() FROM tab WHERE s IN ('word1', NULL) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE s IN ('word1', NULL);

-- tokenbf_v1

DROP TABLE tab;
CREATE TABLE tab (s String, INDEX idx s TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(4);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS force_data_skipping_indices = 'idx';

-- ngrambf_v1

DROP TABLE tab;
CREATE TABLE tab (s String, INDEX idx s TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(4);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';

-- A FixedString element is padded with NUL bytes, so the n-gram tokenizer emits windows over them
-- and the element requires trigrams no String granule stored.
SELECT count() FROM tab WHERE s IN (SELECT toFixedString('word1', 12)) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT (SELECT count() FROM tab WHERE s IN (SELECT toFixedString('word1', 12))) = (SELECT count() FROM tab WHERE s IN (SELECT toFixedString('word1', 12)) SETTINGS use_skip_indexes = 0);

-- A Nullable element type is what transform_null_in = 1 adds, so a null-free set of it must prune.
SELECT count() FROM tab WHERE s IN (SELECT CAST('word1', 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s IN (SELECT CAST(NULL, 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- sparse_grams

DROP TABLE tab;
CREATE TABLE tab (s String, INDEX idx s TYPE sparse_grams(3, 100, 512, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(4);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';

-- A tuple set is one Tuple column unpacked by position, so its element types are unpacked with it.

DROP TABLE tab;
CREATE TABLE tab (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab SELECT number, 'word' || toString(number) FROM numbers(4);

SELECT count() FROM tab WHERE (id, s) IN ((1, 'word1')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE (id, s) IN (SELECT tuple(number, 'word1') FROM numbers(4)) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;
