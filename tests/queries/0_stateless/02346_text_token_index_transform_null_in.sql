-- Tests that the `text`, `tokenbf_v1`, `ngrambf_v1` and `sparse_grams` skip indexes prune an `IN`
-- when `transform_null_in = 1`, where the predicate arrives as `nullIn`/`globalNullIn`, and that a
-- set holding a NULL element is refused because `nullIn` also matches the column's NULL rows.
--
-- `index_granularity = 4` over 8 rows, so every granule is mixed: granule 0 holds word0..word3.

SET enable_full_text_index = 1;
SET transform_null_in = 1;

DROP TABLE IF EXISTS tab;

-- The same block runs for every index type. `tokenbf_v1`, `ngrambf_v1` and `sparse_grams` reject a
-- `Nullable` column at DDL, so the common block uses `String` and NULL enters through the set only.

-- text

CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(8);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- The text index does not model NOT IN at all, unlike the token bloom filter family below.
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
-- Without transform_null_in the same predicates keep the globalIn/globalNotIn spellings, which must prune too.
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
-- A Nullable element type is what transform_null_in = 1 adds, so a null-free set of it must prune.
SELECT count() FROM tab WHERE s IN (SELECT CAST('word1', 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s IN (SELECT CAST(NULL, 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- tokenbf_v1

DROP TABLE tab;
CREATE TABLE tab (s String, INDEX idx s TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(8);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- NOT IN never prunes on these indexes, but the index must still be used.
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- Without transform_null_in the same predicates keep the globalIn/globalNotIn spellings, which must prune too.
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
-- A Nullable element type is what transform_null_in = 1 adds, so a null-free set of it must prune.
SELECT count() FROM tab WHERE s IN (SELECT CAST('word1', 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s IN (SELECT CAST(NULL, 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- ngrambf_v1

DROP TABLE tab;
CREATE TABLE tab (s String, INDEX idx s TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(8);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- NOT IN never prunes on these indexes, but the index must still be used.
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- Without transform_null_in the same predicates keep the globalIn/globalNotIn spellings, which must prune too.
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
-- A Nullable element type is what transform_null_in = 1 adds, so a null-free set of it must prune.
SELECT count() FROM tab WHERE s IN (SELECT CAST('word1', 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s IN (SELECT CAST(NULL, 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A FixedString element has its NUL padding stripped before tokenization, so it keeps pruning.
SELECT count() FROM tab WHERE s IN (SELECT toFixedString('word1', 12)) SETTINGS force_data_skipping_indices = 'idx';
SELECT (SELECT count() FROM tab WHERE s IN (SELECT toFixedString('word1', 12))) = (SELECT count() FROM tab WHERE s IN (SELECT toFixedString('word1', 12)) SETTINGS use_skip_indexes = 0);

-- sparse_grams

DROP TABLE tab;
CREATE TABLE tab (s String, INDEX idx s TYPE sparse_grams(3, 100, 512, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO tab SELECT 'word' || toString(number) FROM numbers(8);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- NOT IN never prunes on these indexes, but the index must still be used.
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS force_data_skipping_indices = 'idx';
-- Without transform_null_in the same predicates keep the globalIn/globalNotIn spellings, which must prune too.
SELECT count() FROM tab WHERE s GLOBAL IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s GLOBAL NOT IN ('word1') SETTINGS transform_null_in = 0, force_data_skipping_indices = 'idx';
-- A Nullable element type is what transform_null_in = 1 adds, so a null-free set of it must prune.
SELECT count() FROM tab WHERE s IN (SELECT CAST('word1', 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE s IN (SELECT CAST(NULL, 'Nullable(String)')) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A Nullable column: only the text index accepts one, and only there can a NULL element of the set
-- meet a NULL row of the column. The set is a disjunction, so one NULL element refuses the index
-- whatever else the set holds.

DROP TABLE tab;
CREATE TABLE tab (s Nullable(String), INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO tab SELECT if(number = 7, NULL, 'word' || toString(number)) FROM numbers(8);

SELECT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE s IN ('word1')) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM tab WHERE s IN ('word1', NULL) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE s IN ('word1', NULL);
SELECT count() FROM tab WHERE s IN ('word1', NULL, 'word2') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE s IN ('word1', NULL, 'word2');

-- A tuple set is one Tuple column unpacked by position, so the indexed component is addressed by it.

DROP TABLE tab;
CREATE TABLE tab (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO tab SELECT number, 'word' || toString(number) FROM numbers(8);

SELECT count() FROM tab WHERE (id, s) IN ((1, 'word1')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE (id, s) IN (SELECT tuple(number, 'word1') FROM numbers(8)) SETTINGS force_data_skipping_indices = 'idx';

-- An absent map key reads the value type's default, which mapValues never stores. The FixedString default
-- is all NUL and the array tokenizer keeps the padding, so it must be recognised as the default.

DROP TABLE tab;
CREATE TABLE tab (id UInt64, m Map(String, FixedString(4)), INDEX idx mapValues(m) TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab VALUES (0, {'k':'val0'}), (1, {'other':'xxxx'}), (2, {'k':'val2'}), (3, {});

SELECT count() FROM tab WHERE m['k'] IN (toFixedString('', 4)) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE m['k'] IN (toFixedString('', 4));
SELECT count() FROM tab WHERE m['k'] = toFixedString('', 4) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE m['k'] = toFixedString('', 4);
SELECT count() FROM tab WHERE m['k'] IN ('val0') SETTINGS force_data_skipping_indices = 'idx';

-- A String map value defaults to '' only, so an all-NUL value is a real value and must prune.

DROP TABLE tab;
CREATE TABLE tab (id UInt64, m Map(String, String), INDEX idx mapValues(m) TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab SELECT 0, map('k', unhex('00'));
INSERT INTO tab SELECT number, map('k', 'val' || toString(number)) FROM numbers(1, 2);
INSERT INTO tab SELECT 3, map('other', 'x');

SELECT count() FROM tab WHERE m['k'] = unhex('00') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE m['k'] IN (unhex('00')) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE m['k'] = '' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE m['k'] IN ('') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab WHERE m['k'] = '';

DROP TABLE tab;
