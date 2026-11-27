-- A text-search function on `m['k']` applies the preprocessor of a `mapValues(m)` text index to the element and to
-- the search term, as for a `String` column, so the index skips only rows the function rejects.

SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab_substr;
DROP TABLE IF EXISTS tab_fixed;
DROP TABLE IF EXISTS tab_lower;
DROP TABLE IF EXISTS tab_soundex;
DROP TABLE IF EXISTS tab_throw;
DROP TABLE IF EXISTS tab_throw_absent;
DROP TABLE IF EXISTS tab_post;
DROP TABLE IF EXISTS tab_lc;
DROP TABLE IF EXISTS tab_nullable;
DROP TABLE IF EXISTS tab_lc_type;
DROP TABLE IF EXISTS tab_sep;
DROP TABLE IF EXISTS tab_post_empty;
DROP TABLE IF EXISTS tab_post_empty_str;
DROP TABLE IF EXISTS tab_null_key;

CREATE TABLE tab_substr (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = substringIndex(mapValues(m), '\n', 1)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_substr VALUES (1, map('k', 'header line\nHello there')), (2, map('k2', 'Hello')), (3, map());

SELECT 'substringIndex', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAllTokens(m['k'], 'header line') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasToken(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasPhrase(m['k'], 'Hello there') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'header') ORDER BY id)),
    (SELECT groupArray(h) FROM (SELECT hasAnyTokens(m['k'], 'Hello') AS h FROM tab_substr ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'substringIndex', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAllTokens(m['k'], 'header line') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasToken(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasPhrase(m['k'], 'Hello there') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'header') ORDER BY id)),
    (SELECT groupArray(h) FROM (SELECT hasAnyTokens(m['k'], 'Hello') AS h FROM tab_substr ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'substringIndex', 'no direct read',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAllTokens(m['k'], 'header line') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasToken(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasPhrase(m['k'], 'Hello there') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'header') ORDER BY id)),
    (SELECT groupArray(h) FROM (SELECT hasAnyTokens(m['k'], 'Hello') AS h FROM tab_substr ORDER BY id))
SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'substringIndex', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAllTokens(m['k'], 'header line') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasToken(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasPhrase(m['k'], 'Hello there') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_substr WHERE hasAnyTokens(m['k'], 'header') ORDER BY id)),
    (SELECT groupArray(h) FROM (SELECT hasAnyTokens(m['k'], 'Hello') AS h FROM tab_substr ORDER BY id))
SETTINGS use_skip_indexes = 0;

CREATE TABLE tab_fixed (id UInt32, m Map(String, FixedString(5)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = substring(mapValues(m), 1, 3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_fixed VALUES (1, map('k', 'x abc'));

SELECT 'substring',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], 'abc') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], 'x') ORDER BY id));
SELECT 'substring', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], 'abc') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], 'x') ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- A case-insensitive index matches any case of the term and still skips granules.
CREATE TABLE tab_lower (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_lower VALUES (1, map('k', 'Hello')), (2, map('k', 'other'));

SELECT 'lower', groupArray(id) FROM (SELECT id FROM tab_lower WHERE hasAnyTokens(m['k'], 'HELLO') ORDER BY id);
SELECT 'lower', 'no index', groupArray(id) FROM (SELECT id FROM tab_lower WHERE hasAnyTokens(m['k'], 'HELLO') ORDER BY id)
SETTINGS use_skip_indexes = 0;
SELECT 'lower pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_lower WHERE hasAnyTokens(m['k'], 'HELLO')) WHERE explain LIKE '%Granules: 1/2%';

-- A row without the key reads '', which `soundex` maps to '0000', the same as '123'.
CREATE TABLE tab_soundex (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = soundex(mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_soundex VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'soundex',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasToken(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasPhrase(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasAnyTokens(m['k'], 'hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasToken(m['k'], '') ORDER BY id));
SELECT 'soundex', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasToken(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasPhrase(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasAnyTokens(m['k'], 'hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_soundex WHERE hasToken(m['k'], '') ORDER BY id))
SETTINGS use_skip_indexes = 0;
SELECT 'soundex pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_soundex WHERE hasAnyTokens(m['k'], 'hello')) WHERE explain LIKE '%Granules: 1/2%';

-- The preprocessor raises on '', the value a row without the key reads.
CREATE TABLE tab_throw (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = toString(toUInt64(mapValues(m)))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_throw VALUES (1, map('k', '0123')), (2, map('k', '456'));
CREATE TABLE tab_throw_absent (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = toString(toUInt64(mapValues(m)))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_throw_absent VALUES (1, map()), (2, map('k', '0123'));

SELECT 'throw', groupArray(id) FROM (SELECT id FROM tab_throw WHERE hasAnyTokens(m['k'], '123') ORDER BY id);
SELECT 'throw', 'no index', groupArray(id) FROM (SELECT id FROM tab_throw WHERE hasAnyTokens(m['k'], '123') ORDER BY id)
SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_throw_absent WHERE hasAnyTokens(m['k'], '123')); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT groupArray(id) FROM (SELECT id FROM tab_throw_absent WHERE hasAnyTokens(m['k'], '123'))
SETTINGS use_skip_indexes = 0; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

CREATE TABLE tab_post (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = substringIndex(mapValues(m), '\n', 1),
        postprocessor = upper(mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_post VALUES (1, map('k', 'header line\nHello there'));

SELECT 'postprocessor',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], 'hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], 'Line') ORDER BY id));
SELECT 'postprocessor', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], 'hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], 'Line') ORDER BY id))
SETTINGS use_skip_indexes = 0;

CREATE TABLE tab_lc (id UInt32, m Map(String, LowCardinality(String)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = substringIndex(mapValues(m), '\n', 1)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_lc VALUES (1, map('k', 'header line\nHello there')), (2, map());
CREATE TABLE tab_nullable (id UInt32, m Map(String, Nullable(String)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = substringIndex(mapValues(m), '\n', 1)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_nullable VALUES (1, map('k', 'header line\nHello there')), (2, map());

SELECT 'lc nullable',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc WHERE hasAnyTokens(m['k'], 'header') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_nullable WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_nullable WHERE hasAnyTokens(m['k'], 'header') ORDER BY id));
SELECT 'lc nullable', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc WHERE hasAnyTokens(m['k'], 'header') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_nullable WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_nullable WHERE hasAnyTokens(m['k'], 'header') ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- The index preprocesses a `LowCardinality(String)` map value as `String`, also when it is read as the subcolumn `m.key_k`.
CREATE TABLE tab_lc_type (id UInt32, m Map(String, LowCardinality(String)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha,
        preprocessor = if(toTypeName(mapValues(m)) = 'LowCardinality(String)', 'hit', substringIndex(mapValues(m), '\n', 1))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_lc_type VALUES (1, map('k', 'other\nzzz')), (2, map());

SELECT 'lc type',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasAnyTokens(m.key_k, 'hit') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasAllTokens(m.key_k, 'zzz') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasToken(m.key_k, 'other') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasPhrase(m.key_k, 'other') ORDER BY id));
SELECT 'lc type', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasAnyTokens(m.key_k, 'hit') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasAllTokens(m.key_k, 'zzz') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasToken(m.key_k, 'other') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_lc_type WHERE hasPhrase(m.key_k, 'other') ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- The preprocessed `hasToken` term 'xyz zz' contains a separator, so the function raises, although no row holds 'xyz'.
-- A row without the key reads ' zz', which matches 'zz'.
CREATE TABLE tab_sep (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = concat(mapValues(m), ' zz')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_sep VALUES (1, map('k', 'abc')), (2, map());

SELECT groupArray(id) FROM (SELECT id FROM tab_sep WHERE hasToken(m['k'], 'xyz')); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(id) FROM (SELECT id FROM tab_sep WHERE hasToken(m['k'], 'xyz'))
SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT 'separator', groupArray(id) FROM (SELECT id FROM tab_sep WHERE hasAnyTokens(m['k'], 'zz') ORDER BY id);
SELECT 'separator', 'no index', groupArray(id) FROM (SELECT id FROM tab_sep WHERE hasAnyTokens(m['k'], 'zz') ORDER BY id)
SETTINGS use_skip_indexes = 0;

-- A postprocessor turning '' into a token must not make an empty `hasToken` term match.
CREATE TABLE tab_post_empty (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(mapValues(m)),
        postprocessor = if(empty(mapValues(m)), 'x', mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_post_empty VALUES (1, map('k', 'X')), (2, map('k', 'y'));

CREATE TABLE tab_post_empty_str (id UInt32, s String,
    INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s), postprocessor = if(empty(s), 'x', s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_post_empty_str VALUES (1, 'X'), (2, 'y');

SELECT 'post empty',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_empty WHERE hasToken(m['k'], '') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_empty_str WHERE hasToken(s, '') ORDER BY id));
SELECT 'post empty', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_empty WHERE hasToken(m['k'], '') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_empty_str WHERE hasToken(s, '') ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- A NULL key reads NULL, which the preprocessor may turn into a match.
CREATE TABLE tab_null_key (id UInt32, m Map(String, String), k Nullable(String),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, preprocessor = ifNull(mapValues(m), 'missing')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_null_key VALUES (1, map('a', 'x'), NULL), (2, map('a', 'missing'), 'a');

SELECT 'null key',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasToken(m[k], 'missing') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasAnyTokens(m[k], 'missing') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasAllTokens(m[k], 'missing') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasPhrase(m[k], 'missing') ORDER BY id));
SELECT 'null key', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasToken(m[k], 'missing') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasAnyTokens(m[k], 'missing') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasAllTokens(m[k], 'missing') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_null_key WHERE hasPhrase(m[k], 'missing') ORDER BY id))
SETTINGS use_skip_indexes = 0;

DROP TABLE tab_substr;
DROP TABLE tab_fixed;
DROP TABLE tab_lower;
DROP TABLE tab_soundex;
DROP TABLE tab_throw;
DROP TABLE tab_throw_absent;
DROP TABLE tab_post;
DROP TABLE tab_lc;
DROP TABLE tab_nullable;
DROP TABLE tab_lc_type;
DROP TABLE tab_sep;
DROP TABLE tab_post_empty;
DROP TABLE tab_post_empty_str;
DROP TABLE tab_null_key;
