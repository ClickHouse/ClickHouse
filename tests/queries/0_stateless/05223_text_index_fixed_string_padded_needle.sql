-- A `FixedString` in a comparison drops trailing zero bytes; the index must look up the padding form it stores or decline.
-- `force_data_skipping_indices` tells pruning from declining: it raises when the index declined the query.

SET allow_experimental_full_text_index = 1;

SELECT 'both padding forms equal the needle';
SELECT concat('hello', unhex('0000000000')) = toFixedString('hello', 10), 'hello' = toFixedString('hello', 10);

DROP TABLE IF EXISTS t_text_padded_needle;
DROP TABLE IF EXISTS t_text_padded_needles;

-- The needle as a table column, the way the issue met it.
CREATE TABLE t_text_padded_needles (fs FixedString(10)) ENGINE = Memory;
INSERT INTO t_text_padded_needles VALUES ('hello');

SELECT '-- array tokenizer on a String column';

CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 10)) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 10)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id);

SELECT '---- a padded needle declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

SELECT '---- a needle that carries no padding still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 5) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- splitByNonAlpha tokenizer on a String column';

-- The zero byte is a separator, so every padding form stores the term `hello` and the stripped needle covers them all.
CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id);

SELECT '---- a padded needle still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- splitByNonAlpha tokenizer with a preprocessor on a String column';

-- A preprocessor may rewrite the padding forms apart, so a padded needle declines the index.
CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);

SELECT '---- a padded needle declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

SELECT '---- a needle that carries no padding still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 5) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- splitByString tokenizer with the zero byte as a separator on a String column';

-- The separators are not inspected, so every `splitByString` index declines a padded needle.
CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByString(['\0'])))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);

SELECT '---- a padded needle declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

SELECT '---- a needle that carries no padding still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 5) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- splitByString tokenizer with another separator on a String column';

-- The zero byte is not a separator here, so the two padding forms are two terms and the index declines.
CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByString([','])))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);

SELECT '---- a padded needle declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

SELECT '---- a needle that carries no padding still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 5) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- array tokenizer on an Array(String) column';

-- `hasAny` and `hasAll` drop only the needle's padding, so the stripped form is the only match.
CREATE TABLE t_text_padded_needle (id UInt32, arr Array(String), INDEX tix arr TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, [concat('hello', unhex('0000000000')), 'foo'];
INSERT INTO t_text_padded_needle SELECT 2, ['world'];
INSERT INTO t_text_padded_needle SELECT 3, ['hello', 'foo'];

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, [toFixedString('hello', 10), toFixedString('foo', 10)]) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, [toFixedString('hello', 10), toFixedString('foo', 10)]) ORDER BY id);

SELECT '---- a padded element still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, [toFixedString('hello', 10), toFixedString('foo', 10)]) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('world', 10)]) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- array tokenizer on a FixedString column';

-- The index stores the values padded to the column width, so the needle is looked up re-padded to it.
CREATE TABLE t_text_padded_needle (id UInt32, s FixedString(6), INDEX tix s TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, 'hello';
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = concat('hello', unhex('00')) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = concat('hello', unhex('00')) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 6) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 6) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 6)) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 6)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello, world', 12) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello, world', 12) ORDER BY id);

SELECT '---- the needle is looked up at the column width and prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'world' ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = concat('hello', unhex('00')) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('world', 6)) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT fs FROM t_text_padded_needles) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

SELECT '---- a needle longer than the column declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello, world', 12) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_text_padded_needle;

SELECT '-- array tokenizer on an Array(FixedString) column';

-- A `String` needle keeps its trailing zero bytes and matches nothing; exact direct read must not answer it.
CREATE TABLE t_text_padded_needle (id UInt32, arr Array(FixedString(6)), INDEX tix arr TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, ['hello', 'foo'];
INSERT INTO t_text_padded_needle SELECT 2, ['world'];
INSERT INTO t_text_padded_needle SELECT 3, ['hello'];

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, ['hello']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, ['hello']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [concat('hello', unhex('00'))]) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [concat('hello', unhex('00'))]) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, ['hello', 'foo']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, ['hello', 'foo']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, 'hello') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, 'hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, toFixedString('hello', 10)) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, toFixedString('hello', 10)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, concat('hello', unhex('00'))) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, concat('hello', unhex('00'))) ORDER BY id);

SELECT '---- the needle is looked up at the column width and prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, ['world']) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, [toFixedString('hello', 10), toFixedString('foo', 10)]) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, 'world') ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, toFixedString('world', 10)) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

SELECT '---- a String needle with a literal trailing zero byte declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [concat('hello', unhex('00'))]) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE has(arr, concat('hello', unhex('00'))) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_text_padded_needle;

SELECT '-- array tokenizer on the keys of a Map(FixedString, UInt8) column';

CREATE TABLE t_text_padded_needle (id UInt32, m Map(FixedString(6), UInt8), INDEX tix mapKeys(m) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, map('hello', 1);
INSERT INTO t_text_padded_needle SELECT 2, map('world', 1);

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, 'hello') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, 'hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, toFixedString('hello', 10)) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, toFixedString('hello', 10)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, concat('hello', unhex('00'))) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, concat('hello', unhex('00'))) ORDER BY id);

SELECT '---- the needle is looked up at the column width and prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, 'world') ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

SELECT '---- a String needle with a literal trailing zero byte declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE mapContainsKey(m, concat('hello', unhex('00'))) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_text_padded_needle;
DROP TABLE t_text_padded_needles;
