-- A `FixedString` needle compares equal to a `String` value spelled with any number of trailing zero
-- bytes, and a term-preserving tokenizer stores every spelling as a different term. One index lookup
-- cannot cover them all, so the index has to decline the atom instead of pruning the granule that holds
-- another spelling. A tokenizer that treats the zero byte as a separator stores one term for all of them
-- and keeps pruning with the stripped needle. `force_data_skipping_indices` tells the two apart: it
-- raises when the index declined the query.

SET allow_experimental_full_text_index = 1;

SELECT 'both spellings equal the needle';
SELECT concat('hello', unhex('0000000000')) = toFixedString('hello', 10), 'hello' = toFixedString('hello', 10);

SELECT '-- array tokenizer on a String column';

DROP TABLE IF EXISTS t_text_padded_needle;

CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 10)) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 10)) ORDER BY id);

SELECT '---- a padded needle declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

SELECT '---- a needle that carries no padding still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 5) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- splitByString tokenizer with the zero byte as a separator on a String column';

-- The padding is split away as a separator, so `'hello'` and `'hello\0\0\0\0\0'` share one term and
-- the padded needle keeps pruning. Decided on the tokenizer instance, not on its kind.
CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByString(['\0'])) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);

SELECT '---- a padded needle still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;

SELECT '-- splitByString tokenizer with another separator on a String column';

-- The zero byte is not a separator here, so the two spellings are two terms again and the index declines.
CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByString([','])) GRANULARITY 1)
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

-- `hasAny` and `hasAll` cast the needle to `String`, which drops its padding, and compare the value as
-- it is, so only the stripped spelling matches and one lookup covers it.
CREATE TABLE t_text_padded_needle (id UInt32, arr Array(String), INDEX tix arr TYPE text(tokenizer = array) GRANULARITY 1)
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

-- The column stores every value padded to its width, and so does the index, while the comparison drops
-- the padding of both sides. The needle is looked up re-padded to the column width, whatever its own
-- type and width, and a needle longer than the column matches nothing.
CREATE TABLE t_text_padded_needle (id UInt32, s FixedString(6), INDEX tix s TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, 'hello';
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 6) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 6) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 6)) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('hello', 6)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello, world', 12) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello, world', 12) ORDER BY id);

SELECT '---- the needle is looked up at the column width and prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'world' ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 10) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s IN (SELECT toFixedString('world', 6)) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

SELECT '---- a needle longer than the column declines the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello, world', 12) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_text_padded_needle;

SELECT '-- array tokenizer on an Array(FixedString) column';

CREATE TABLE t_text_padded_needle (id UInt32, arr Array(FixedString(6)), INDEX tix arr TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, ['hello', 'foo'];
INSERT INTO t_text_padded_needle SELECT 2, ['world'];
INSERT INTO t_text_padded_needle SELECT 3, ['hello'];

SELECT '---- the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, ['hello']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, ['hello']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, [toFixedString('hello', 10)]) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, ['hello', 'foo']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, ['hello', 'foo']) ORDER BY id);

SELECT '---- the needle is looked up at the column width and prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(arr, ['world']) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAll(arr, [toFixedString('hello', 10), toFixedString('foo', 10)]) ORDER BY id) SETTINGS force_data_skipping_indices = 'tix';

DROP TABLE t_text_padded_needle;
