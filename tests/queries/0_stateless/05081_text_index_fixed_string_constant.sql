-- Fixes https://github.com/ClickHouse/ClickHouse/issues/117021.

-- `String = FixedString(N)` ignores the constant's trailing zero padding, but the index terms were
-- extracted from the padded bytes, so every granule looked unmatched and matching rows disappeared.

SELECT 'Ground truth';
SELECT 'hello' = toFixedString('hello', 10);

SELECT 'Text Index';

SELECT '-- ngrams tokenizer on a String column';

DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_string;

SELECT '-- ngrams tokenizer on a FixedString column';

DROP TABLE IF EXISTS tab_fixed_string;
CREATE TABLE tab_fixed_string
(
    id UInt32,
    s FixedString(6),
    INDEX idx s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_fixed_string;

SELECT '-- splitByNonAlpha tokenizer on a String column';

DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_string;

SELECT '-- splitByNonAlpha tokenizer on a FixedString column';

DROP TABLE IF EXISTS tab_fixed_string;
CREATE TABLE tab_fixed_string
(
    id UInt32,
    s FixedString(6),
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_fixed_string;

SELECT '-- array tokenizer on a String column';

DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_string;

SELECT '-- array tokenizer on a FixedString column';

-- The `array` tokenizer stores the whole padded value as one term, so the constant keeps its padding
-- and only a constant as wide as the column matches.
DROP TABLE IF EXISTS tab_fixed_string;
CREATE TABLE tab_fixed_string
(
    id UInt32,
    s FixedString(6),
    INDEX idx s TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 6);
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 6) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 6));
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 6)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_fixed_string;

SELECT '-- sparseGrams tokenizer on a String column';

DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = sparseGrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_string;

SELECT '-- sparseGrams tokenizer on a FixedString column';

DROP TABLE IF EXISTS tab_fixed_string;
CREATE TABLE tab_fixed_string
(
    id UInt32,
    s FixedString(6),
    INDEX idx s TYPE text(tokenizer = sparseGrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_fixed_string;

SELECT 'Bloom Filter Index';

SELECT '-- tokenbf_v1 on a String column';

DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE tokenbf_v1(512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_string;

SELECT '-- tokenbf_v1 on a FixedString column';

DROP TABLE IF EXISTS tab_fixed_string;
CREATE TABLE tab_fixed_string
(
    id UInt32,
    s FixedString(6),
    INDEX idx s TYPE tokenbf_v1(512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_fixed_string;

SELECT '-- ngrambf_v1 on a String column';

DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_string;

SELECT '-- ngrambf_v1 on a FixedString column';

DROP TABLE IF EXISTS tab_fixed_string;
CREATE TABLE tab_fixed_string
(
    id UInt32,
    s FixedString(6),
    INDEX idx s TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');

SELECT '---- FixedString comparison';
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM tab_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM tab_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_fixed_string;

SELECT 'Functions';

DROP TABLE IF EXISTS tab_string;
DROP TABLE IF EXISTS tab_string_ngrambf;
DROP TABLE IF EXISTS tab_array;
DROP TABLE IF EXISTS tab_array_ngrambf;
DROP TABLE IF EXISTS tab_map;

CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

CREATE TABLE tab_string_ngrambf
(
    id UInt32,
    s String,
    INDEX idx s TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

CREATE TABLE tab_array
(
    id UInt32,
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

CREATE TABLE tab_array_ngrambf
(
    id UInt32,
    arr Array(String),
    INDEX idx arr TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

CREATE TABLE tab_map
(
    id UInt32,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = array),
    INDEX idx_values mapValues(m) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');
INSERT INTO tab_string_ngrambf VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');
INSERT INTO tab_array VALUES (1, ['hello']), (2, ['world']), (3, ['hello']);
INSERT INTO tab_array_ngrambf VALUES (1, ['hello']), (2, ['world']), (3, ['hello']);
INSERT INTO tab_map VALUES (1, map('hello', 'world')), (2, map('foo', 'bar')), (3, map('hello', 'world'));

SELECT '-- hasAny and hasAll ignore the padding';
SELECT count() FROM tab_array WHERE hasAny(arr, [toFixedString('hello', 10)]);
SELECT count() FROM tab_array WHERE hasAny(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab_array WHERE hasAll(arr, [toFixedString('hello', 10)]);
SELECT count() FROM tab_array WHERE hasAll(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab_array_ngrambf WHERE hasAny(arr, [toFixedString('hello', 10)]);
SELECT count() FROM tab_array_ngrambf WHERE hasAny(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_array_ngrambf WHERE hasAll(arr, [toFixedString('hello', 10)]);
SELECT count() FROM tab_array_ngrambf WHERE hasAll(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0;

-- The functions below compare the raw padded bytes, so their terms must keep the padding.
-- `text(tokenizer = array)` answers them by exact direct read, where a stripped term would return
-- rows the predicate rejects.
SELECT '-- has keeps the padding';
SELECT count() FROM tab_array WHERE has(arr, toFixedString('hello', 10));
SELECT count() FROM tab_array WHERE has(arr, toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab_array WHERE has(arr, 'hello');
SELECT count() FROM tab_array WHERE has(arr, 'hello') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT '-- mapContainsKey and mapContainsValue keep the padding';
SELECT count() FROM tab_map WHERE mapContainsKey(m, toFixedString('hello', 10));
SELECT count() FROM tab_map WHERE mapContainsKey(m, toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab_map WHERE mapContainsValue(m, toFixedString('world', 10));
SELECT count() FROM tab_map WHERE mapContainsValue(m, toFixedString('world', 10)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab_map WHERE mapContainsKey(m, 'hello');
SELECT count() FROM tab_map WHERE mapContainsKey(m, 'hello') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT '-- startsWith and endsWith keep the padding';
SELECT count() FROM tab_string WHERE startsWith(s, toFixedString('hel', 10));
SELECT count() FROM tab_string WHERE startsWith(s, toFixedString('hel', 10)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE endsWith(s, toFixedString('llo', 10));
SELECT count() FROM tab_string WHERE endsWith(s, toFixedString('llo', 10)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE startsWith(s, 'hel');
SELECT count() FROM tab_string WHERE startsWith(s, 'hel') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_string WHERE endsWith(s, 'llo');
SELECT count() FROM tab_string WHERE endsWith(s, 'llo') SETTINGS use_skip_indexes = 0;

SELECT '-- an unpadded constant still prunes';
SELECT count() FROM tab_string WHERE s = 'hello';
SELECT count() FROM tab_string WHERE s = 'nosuch';
SELECT count() FROM tab_string_ngrambf WHERE s = 'hello';
SELECT count() FROM tab_string_ngrambf WHERE s = 'nosuch';

DROP TABLE tab_string;
DROP TABLE tab_string_ngrambf;
DROP TABLE tab_array;
DROP TABLE tab_array_ngrambf;
DROP TABLE tab_map;
