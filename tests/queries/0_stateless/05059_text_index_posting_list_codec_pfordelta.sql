-- Tests the `pfordelta` posting list codec against `bitpacking` and `none` on identical data.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET query_plan_optimize_count_from_text_index = 0;

DROP TABLE IF EXISTS tab_src;
DROP TABLE IF EXISTS tab_none;
DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS tab_pfordelta;

CREATE TABLE tab_src (
    id UInt64,
    str String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_src SELECT number, concat(
    'dense',
    if(number <= 128, ' tailblock', ''),
    if(number = 500, ' single', ''),
    if(number IN (0, 777), ' raretwo', ''),
    if(number IN (1, 2, 3, 4, 5), ' rarefive', ''),
    if(number IN (10, 11, 12, 9000), ' outlier', ''))
FROM numbers(10000);

CREATE TABLE tab_none (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 512, posting_list_codec = 'none')
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_bitpacking (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 512, posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_pfordelta (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 512, posting_list_codec = 'pfordelta')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_none SELECT * FROM tab_src WHERE id < 5000;
INSERT INTO tab_none SELECT * FROM tab_src WHERE id >= 5000;

INSERT INTO tab_bitpacking SELECT * FROM tab_src WHERE id < 5000;
INSERT INTO tab_bitpacking SELECT * FROM tab_src WHERE id >= 5000;

INSERT INTO tab_pfordelta SELECT * FROM tab_src WHERE id < 5000;
INSERT INTO tab_pfordelta SELECT * FROM tab_src WHERE id >= 5000;

SELECT 'Before merge, materialize';
SET text_index_posting_list_apply_mode = 'materialize';

SELECT count() FROM tab_none WHERE hasToken(str, 'dense');
SELECT count() FROM tab_none WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_none WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_none WHERE hasToken(str, 'single');
SELECT count() FROM tab_none WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_none WHERE hasToken(str, 'rarefive');

SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'dense');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'single');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'rarefive');

SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'single');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'rarefive');

SELECT 'Before merge, lazy';
SET text_index_posting_list_apply_mode = 'lazy';

SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'single');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'rarefive');

SELECT 'Intersection';
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense') AND hasToken(str, 'outlier');

SELECT 'Row ids';
SELECT arraySort(groupArray(id)) FROM tab_pfordelta WHERE hasToken(str, 'outlier');
SELECT arraySort(groupArray(id)) FROM tab_pfordelta WHERE hasToken(str, 'rarefive');

SELECT 'After merge, materialize';

OPTIMIZE TABLE tab_none FINAL;
OPTIMIZE TABLE tab_bitpacking FINAL;
OPTIMIZE TABLE tab_pfordelta FINAL;

SET text_index_posting_list_apply_mode = 'materialize';

SELECT count() FROM tab_none WHERE hasToken(str, 'dense');
SELECT count() FROM tab_none WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_none WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_none WHERE hasToken(str, 'single');
SELECT count() FROM tab_none WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_none WHERE hasToken(str, 'rarefive');

SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'dense');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'single');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'rarefive');

SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'single');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'rarefive');

SELECT 'After merge, lazy';
SET text_index_posting_list_apply_mode = 'lazy';

SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'tailblock');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'single');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'raretwo');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'rarefive');

SELECT 'Intersection';
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense') AND hasToken(str, 'outlier');

SELECT 'Row ids';
SELECT arraySort(groupArray(id)) FROM tab_pfordelta WHERE hasToken(str, 'outlier');
SELECT arraySort(groupArray(id)) FROM tab_pfordelta WHERE hasToken(str, 'rarefive');

DROP TABLE tab_none;
DROP TABLE tab_bitpacking;
DROP TABLE tab_pfordelta;

SELECT 'Table setting';

DROP TABLE IF EXISTS tab_setting;

CREATE TABLE tab_setting (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfordelta')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_setting SELECT id, if(id % 7 = 0, 'seven', 'other') FROM tab_src;
OPTIMIZE TABLE tab_setting FINAL;

SELECT count() FROM tab_setting WHERE hasToken(str, 'seven');
SELECT count() FROM tab_setting WHERE hasToken(str, 'other');

DROP TABLE tab_setting;
DROP TABLE tab_src;

SELECT 'Mixed codec merge';

DROP TABLE IF EXISTS tab_mixed;

CREATE TABLE tab_mixed (
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple();

SYSTEM STOP MERGES tab_mixed;

ALTER TABLE tab_mixed MODIFY SETTING text_index_posting_list_codec = 'bitpacking';

INSERT INTO tab_mixed SELECT 'hello world ' || toString(number) FROM numbers(1000);

ALTER TABLE tab_mixed MODIFY SETTING text_index_posting_list_codec = 'pfordelta';

INSERT INTO tab_mixed SELECT 'foo bar ' || toString(number) FROM numbers(1000);

ALTER TABLE tab_mixed MODIFY SETTING text_index_posting_list_codec = 'none';

INSERT INTO tab_mixed SELECT 'baz qux ' || toString(number) FROM numbers(1000);

SYSTEM START MERGES tab_mixed;
OPTIMIZE TABLE tab_mixed FINAL;

SELECT count() FROM tab_mixed;
SELECT count() FROM tab_mixed WHERE hasToken(str, 'hello');
SELECT count() FROM tab_mixed WHERE hasToken(str, 'foo');
SELECT count() FROM tab_mixed WHERE hasToken(str, 'baz');

DROP TABLE tab_mixed;
