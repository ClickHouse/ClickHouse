-- A row whose map has no key `k` reads the value type's default for `m['k']`. A text index on `mapValues(m)` must
-- not skip it when the index tokenizer or postprocessor make that default match.

SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab_post;
DROP TABLE IF EXISTS tab_post_lc;
DROP TABLE IF EXISTS tab_phrase;
DROP TABLE IF EXISTS tab_phrase_pos;
DROP TABLE IF EXISTS tab_split_pos;
DROP TABLE IF EXISTS tab_ngrams;
DROP TABLE IF EXISTS tab_pre_lower;
DROP TABLE IF EXISTS tab_pre_nul;
DROP TABLE IF EXISTS tab_string;
DROP TABLE IF EXISTS tab_nullable;

-- The postprocessor maps every token without a lowercase letter to '#', including the default's '\0\0\0'.
-- The last column adds a condition the index is used for.
CREATE TABLE tab_post (id UInt32, m Map(String, FixedString(6)),
    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_post VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'post', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') AND NOT hasAnyTokens(m['k'], 'hel') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'post', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') AND NOT hasAnyTokens(m['k'], 'hel') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'post', 'no hint',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') AND NOT hasAnyTokens(m['k'], 'hel') ORDER BY id))
SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'post', 'no direct read',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') AND NOT hasAnyTokens(m['k'], 'hel') ORDER BY id))
SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'post', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], '123') AND NOT hasAnyTokens(m['k'], 'hel') ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- A needle the default does not produce still prunes.
SELECT 'post control', groupArray(id) FROM (SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], 'hel') ORDER BY id);
SELECT 'post control pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_post WHERE hasAnyTokens(m['k'], 'hel')) WHERE explain LIKE '%Granules: 1/2%';

CREATE TABLE tab_post_lc (id UInt32, m Map(String, LowCardinality(FixedString(6))),
    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_post_lc VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'post lc', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_lc WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_lc WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'post lc', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_lc WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_lc WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'post lc', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_lc WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_post_lc WHERE hasAllTokens(m['k'], '123', 'ngrams(3)') ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- The postprocessor maps every token that is not all lowercase letters to 'x'.
CREATE TABLE tab_phrase (id UInt32, m Map(String, FixedString(5)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByString(['abc']), postprocessor = if(match(mapValues(m), '^[a-z]+$'), mapValues(m), 'x')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_phrase VALUES (1, map()), (2, map('k', 'hello')), (3, map('k', '12345'));

SELECT 'phrase', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'phrase', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'phrase', 'no hint',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'phrase', 'no direct read',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'phrase', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS use_skip_indexes = 0;

SELECT 'phrase control', groupArray(id) FROM (SELECT id FROM tab_phrase WHERE hasPhrase(m['k'], 'hello') ORDER BY id);

CREATE TABLE tab_phrase_pos (id UInt32, m Map(String, FixedString(5)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByString(['abc']), support_phrase_search = 1,
        postprocessor = if(match(mapValues(m), '^[a-z]+$'), mapValues(m), 'x')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, allow_experimental_text_index_phrase_search = 1;
INSERT INTO tab_phrase_pos VALUES (1, map()), (2, map('k', 'hello')), (3, map('k', '12345'));

SELECT 'phrase pos', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'phrase pos', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'phrase pos', 'no hint',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'phrase pos', 'no direct read',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'phrase pos', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], '00', 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS use_skip_indexes = 0;

SELECT 'phrase pos control', groupArray(id) FROM (SELECT id FROM tab_phrase_pos WHERE hasPhrase(m['k'], 'hello') ORDER BY id);

-- No postprocessor: the default's only token is five zero bytes.
CREATE TABLE tab_split_pos (id UInt32, m Map(String, FixedString(5)),
    INDEX tix mapValues(m) TYPE text(tokenizer = splitByString(['abc']), support_phrase_search = 1))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, allow_experimental_text_index_phrase_search = 1;
INSERT INTO tab_split_pos VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'split pos', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc')) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'split pos', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc')) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'split pos', 'no hint',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc')) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'split pos', 'no direct read',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc')) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'split pos', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc')) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id))
SETTINGS use_skip_indexes = 0;

SELECT 'split pos control', groupArray(id) FROM (SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], 'hello') ORDER BY id);
SELECT 'split pos control pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_split_pos WHERE hasPhrase(m['k'], 'hello')) WHERE explain LIKE '%Granules: 1/2%';

-- No postprocessor: `ngrams(3)` keeps the default's zero bytes in its tokens.
CREATE TABLE tab_ngrams (id UInt32, m Map(String, FixedString(6)), INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_ngrams VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'ngrams', 'subcolumns=1',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0)), 'ngrams(3)') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'ngrams', 'subcolumns=0',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0)), 'ngrams(3)') ORDER BY id))
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'ngrams', 'no hint',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0)), 'ngrams(3)') ORDER BY id))
SETTINGS query_plan_text_index_add_hint = 0;
SELECT 'ngrams', 'no direct read',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0)), 'ngrams(3)') ORDER BY id))
SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'ngrams', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0)), 'ngrams(3)') ORDER BY id))
SETTINGS use_skip_indexes = 0;

SELECT 'ngrams control pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_ngrams WHERE hasAnyTokens(m['k'], 'hel')) WHERE explain LIKE '%Granules: 1/2%';
SELECT 'ngrams phrase control', groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasPhrase(m['k'], 'hello') ORDER BY id);
SELECT 'ngrams phrase control pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_ngrams WHERE hasPhrase(m['k'], 'hello')) WHERE explain LIKE '%Granules: 1/2%';

-- The preprocessor is applied to the indexed values only, never to `m['k']`.
CREATE TABLE tab_pre_lower (id UInt32, m Map(String, FixedString(6)),
    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), preprocessor = lower(mapValues(m)), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_pre_lower VALUES (1, map()), (2, map('k', 'hello'));

CREATE TABLE tab_pre_nul (id UInt32, m Map(String, FixedString(6)),
    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), preprocessor = replaceAll(mapValues(m), char(0), 'z')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_pre_nul VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'preprocessor',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_pre_lower WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_pre_lower WHERE hasAnyTokens(m['k'], 'hel') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_pre_nul WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id));
SELECT 'preprocessor', 'no index',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_pre_lower WHERE hasAnyTokens(m['k'], '123') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_pre_lower WHERE hasAnyTokens(m['k'], 'hel') ORDER BY id)),
    (SELECT groupArray(id) FROM (SELECT id FROM tab_pre_nul WHERE hasAnyTokens(m['k'], concat('a', char(0), char(0), char(0))) ORDER BY id))
SETTINGS use_skip_indexes = 0;

-- The default of these value types produces no term, so the index still prunes.
CREATE TABLE tab_string (id UInt32, m Map(String, String),
    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_string VALUES (1, map()), (2, map('k', 'hello'));

CREATE TABLE tab_nullable (id UInt32, m Map(String, Nullable(FixedString(6))),
    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_nullable VALUES (1, map()), (2, map('k', 'hello'));

SELECT 'string', groupArray(id) FROM (SELECT id FROM tab_string WHERE hasAnyTokens(m['k'], '123') ORDER BY id);
SELECT 'string', 'no index', groupArray(id) FROM (SELECT id FROM tab_string WHERE hasAnyTokens(m['k'], '123') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT 'string pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_string WHERE hasAnyTokens(m['k'], '123')) WHERE explain LIKE '%Granules: 0/2%';
SELECT 'nullable', groupArray(id) FROM (SELECT id FROM tab_nullable WHERE hasAnyTokens(m['k'], '123') ORDER BY id);
SELECT 'nullable pruned', count() > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_nullable WHERE hasAnyTokens(m['k'], '123')) WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE tab_post;
DROP TABLE tab_post_lc;
DROP TABLE tab_phrase;
DROP TABLE tab_phrase_pos;
DROP TABLE tab_split_pos;
DROP TABLE tab_ngrams;
DROP TABLE tab_pre_lower;
DROP TABLE tab_pre_nul;
DROP TABLE tab_string;
DROP TABLE tab_nullable;
