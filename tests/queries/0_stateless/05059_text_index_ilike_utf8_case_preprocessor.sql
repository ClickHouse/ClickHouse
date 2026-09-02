-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

-- https://github.com/ClickHouse/ClickHouse/issues/116970
-- The default-on `use_text_index_like_evaluation_by_dictionary_scan` answers `ILIKE '%needle%'` from
-- the index token dictionary in `Exact` mode, with no row-level recheck. That is only sound when the
-- preprocessor's case folding is the same one `ILIKE` applies to the raw column. `lowerUTF8`/
-- `upperUTF8` fold non-ASCII code points onto ASCII letters - U+212A KELVIN SIGN becomes `k` - so the
-- dictionary token contained the needle while the raw string does not match, and the query returned
-- phantom rows.

DROP TABLE IF EXISTS t_ilike_utf8_lower;
CREATE TABLE t_ilike_utf8_lower (id UInt32, s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lowerUTF8(s)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ilike_utf8_lower VALUES
    (1, concat('ab', char(0xE2,0x84,0xAA), 'oop zzz')),
    (2, 'abkoop zzz'),
    (3, 'nothing here');

SELECT 'ground truth';
SELECT id, s ILIKE '%abkoop%' FROM t_ilike_utf8_lower ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT 'lowerUTF8 preprocessor';
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_utf8_lower WHERE s ILIKE '%abkoop%' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_utf8_lower WHERE s ILIKE '%abkoop%' ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_utf8_lower WHERE s ILIKE '%abkoop%' ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0);

SELECT 'upperUTF8 preprocessor';
DROP TABLE IF EXISTS t_ilike_utf8_upper;
CREATE TABLE t_ilike_utf8_upper (id UInt32, s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = upperUTF8(s)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ilike_utf8_upper VALUES
    (1, concat('ab', char(0xC5,0xBF), 'oop zzz')),
    (2, 'absoop zzz'),
    (3, 'nothing here');
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_utf8_upper WHERE s ILIKE '%absoop%' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_utf8_upper WHERE s ILIKE '%absoop%' ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT 'ascii lower preprocessor keeps the optimization';
DROP TABLE IF EXISTS t_ilike_ascii_lower;
CREATE TABLE t_ilike_ascii_lower (id UInt32, s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ilike_ascii_lower VALUES (1, 'ABKOOP zzz'), (2, 'abkoop zzz'), (3, 'nothing here');
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_ascii_lower WHERE s ILIKE '%abkoop%' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_ilike_ascii_lower WHERE s ILIKE '%abkoop%' ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_ilike_ascii_lower WHERE s ILIKE '%nosuch%') WHERE explain LIKE '%Granules: 0/%';

DROP TABLE t_ilike_utf8_lower;
DROP TABLE t_ilike_utf8_upper;
DROP TABLE t_ilike_ascii_lower;
