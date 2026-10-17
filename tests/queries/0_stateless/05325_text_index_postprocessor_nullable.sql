-- Text search functions over a Nullable value return NULL for a NULL row when the text index has a postprocessor,
-- exactly as without one: each `tn*` table is the twin of a `tp*` table, with the same index minus the postprocessor.

DROP TABLE IF EXISTS tp;
DROP TABLE IF EXISTS tn;
DROP TABLE IF EXISTS tp_lower;
DROP TABLE IF EXISTS tn_lower;
DROP TABLE IF EXISTS tp_ifnull;
DROP TABLE IF EXISTS tn_ifnull;
DROP TABLE IF EXISTS tp_nullif;
DROP TABLE IF EXISTS tn_nullif;
DROP TABLE IF EXISTS tp_nullif_str;
DROP TABLE IF EXISTS tp_nullif_fs;
DROP TABLE IF EXISTS tp_lc;
DROP TABLE IF EXISTS tn_lc;
DROP TABLE IF EXISTS tp_fs;
DROP TABLE IF EXISTS tn_fs;
DROP TABLE IF EXISTS tp_map;
DROP TABLE IF EXISTS tn_map;
DROP TABLE IF EXISTS tp_partial;
DROP TABLE IF EXISTS tp_afs;
DROP TABLE IF EXISTS tn_afs;
DROP TABLE IF EXISTS tp_mfs;
DROP TABLE IF EXISTS tn_mfs;
DROP TABLE IF EXISTS tp_alc;
DROP TABLE IF EXISTS tn_alc;
DROP TABLE IF EXISTS tp_apre;
DROP TABLE IF EXISTS tn_apre;
DROP TABLE IF EXISTS tp_fs_num;
DROP TABLE IF EXISTS tn_fs_num;
DROP TABLE IF EXISTS tp_lcfs_num;
DROP TABLE IF EXISTS tn_lcfs_num;

SELECT '1. Nullable(String), no preprocessor.';

CREATE TABLE tp (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tn VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');

-- hasToken(s, 'HELLO') = 1 on `tp` only: the postprocessor rewrite ran.
SELECT 'tp', id, hasToken(s, 'HELLO'), hasAnyTokens(s, 'hello'), hasAllTokens(s, 'hello world'), hasPhrase(s, 'hello world') FROM tp ORDER BY id;
SELECT 'tn', id, hasToken(s, 'HELLO'), hasAnyTokens(s, 'hello'), hasAllTokens(s, 'hello world'), hasPhrase(s, 'hello world') FROM tn ORDER BY id;

SELECT 'tp NOT hasToken', arraySort(groupArray(id)) FROM tp WHERE NOT hasToken(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT 'tn NOT hasToken', arraySort(groupArray(id)) FROM tn WHERE NOT hasToken(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT 'tp NOT hasAnyTokens', arraySort(groupArray(id)) FROM tp WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT 'tn NOT hasAnyTokens', arraySort(groupArray(id)) FROM tn WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT 'tp NOT hasAllTokens', arraySort(groupArray(id)) FROM tp WHERE NOT hasAllTokens(s, 'hello world') SETTINGS use_skip_indexes = 0;
SELECT 'tn NOT hasAllTokens', arraySort(groupArray(id)) FROM tn WHERE NOT hasAllTokens(s, 'hello world') SETTINGS use_skip_indexes = 0;
SELECT 'tp NOT hasPhrase', arraySort(groupArray(id)) FROM tp WHERE NOT hasPhrase(s, 'hello world') SETTINGS use_skip_indexes = 0;
SELECT 'tn NOT hasPhrase', arraySort(groupArray(id)) FROM tn WHERE NOT hasPhrase(s, 'hello world') SETTINGS use_skip_indexes = 0;

SELECT 'tp NOT, direct read off', arraySort(groupArray(id)) FROM tp WHERE NOT hasAnyTokens(s, 'hello') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'tn NOT, direct read off', arraySort(groupArray(id)) FROM tn WHERE NOT hasAnyTokens(s, 'hello') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'tp IS NULL, direct read off', arraySort(groupArray(id)) FROM tp WHERE hasAnyTokens(s, 'hello') IS NULL SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'tn IS NULL, direct read off', arraySort(groupArray(id)) FROM tn WHERE hasAnyTokens(s, 'hello') IS NULL SETTINGS query_plan_direct_read_from_text_index = 0;
-- The uppercase needle matches row 2 only through the postprocessor, so `tp merge()` also shows the rewrite ran.
SELECT 'tp merge()', arraySort(groupArray(id)) FROM merge(currentDatabase(), '^tp$') WHERE NOT hasAnyTokens(s, 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'tn merge()', arraySort(groupArray(id)) FROM merge(currentDatabase(), '^tn$') WHERE NOT hasAnyTokens(s, 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '2. Nullable(String) with a preprocessor, applied to the haystack of an analyzed index.';

CREATE TABLE tp_lower (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s), postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_lower (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tp_ifnull (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = ifNull(s, ''), postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_ifnull (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = ifNull(s, '')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tp_nullif (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = nullIf(s, ''), postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_nullif (id UInt32, s Nullable(String), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = nullIf(s, '')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_lower VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tn_lower VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tp_ifnull VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tn_ifnull VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tp_nullif VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tn_nullif VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');

SELECT 'tp_lower', arraySort(groupArray(id)) FROM tp_lower WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'tn_lower', arraySort(groupArray(id)) FROM tn_lower WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
-- The preprocessor removes the NULL, so the NULL row is an ordinary non-match.
SELECT 'tp_ifnull', arraySort(groupArray(id)) FROM tp_ifnull WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'tn_ifnull', arraySort(groupArray(id)) FROM tn_ifnull WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
-- The preprocessor makes the empty row NULL too.
SELECT 'tp_nullif', arraySort(groupArray(id)) FROM tp_nullif WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'tn_nullif', arraySort(groupArray(id)) FROM tn_nullif WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

SELECT '3. String and FixedString with a NULL-producing preprocessor: the predicate is not Nullable.';

CREATE TABLE tp_nullif_str (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByNonAlpha, preprocessor = nullIf(s, ''), postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_nullif_str VALUES (2, 'hello world'), (3, 'foo'), (4, '');
SELECT 'tp_nullif_str', arraySort(groupArray(id)) FROM tp_nullif_str WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
CREATE TABLE tp_nullif_fs (id UInt32, s FixedString(6), INDEX tix s TYPE text(tokenizer = ngrams(3), preprocessor = nullIf(s, '123456'), postprocessor = if(match(s, '[a-z]'), s, '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_nullif_fs VALUES (1, '123456'), (2, 'hello'), (3, '678900');
-- Row 1 becomes NULL, which has no tokens. Row 3 matches through the postprocessor: '999' and its tokens all map to '#'.
SELECT 'tp_nullif_fs', arraySort(groupArray(id)) FROM tp_nullif_fs WHERE NOT hasAnyTokens(s, '999') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

SELECT '4. LowCardinality(Nullable(String)).';

CREATE TABLE tp_lc (id UInt32, s LowCardinality(Nullable(String)), INDEX tix s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_lc (id UInt32, s LowCardinality(Nullable(String)), INDEX tix s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_lc VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
INSERT INTO tn_lc VALUES (1, NULL), (2, 'hello world'), (3, 'foo'), (4, '');
SELECT 'tp_lc', id, hasAnyTokens(s, 'hello'), toTypeName(hasAnyTokens(s, 'hello')) FROM tp_lc ORDER BY id;
SELECT 'tn_lc', id, hasAnyTokens(s, 'hello'), toTypeName(hasAnyTokens(s, 'hello')) FROM tn_lc ORDER BY id;
SELECT 'tp_lc NOT', arraySort(groupArray(id)) FROM tp_lc WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT 'tn_lc NOT', arraySort(groupArray(id)) FROM tn_lc WHERE NOT hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;

SELECT '5. Nullable(FixedString): the nested default of a NULL must not produce tokens.';

CREATE TABLE tp_fs (id UInt32, s Nullable(FixedString(6)), INDEX tix s TYPE text(tokenizer = ngrams(3), postprocessor = if(match(s, '[a-z]'), s, '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_fs (id UInt32, s Nullable(FixedString(6)), INDEX tix s TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_fs VALUES (1, NULL), (2, 'hello'), (3, '12345');
INSERT INTO tn_fs VALUES (1, NULL), (2, 'hello'), (3, '12345');
-- '999' has no letter, so the postprocessor maps it, like every letter-free token, to '#': '12345' matches only through the rewrite.
SELECT 'tp_fs', id, hasAnyTokens(s, '999') FROM tp_fs ORDER BY id;
SELECT 'tn_fs', id, hasAnyTokens(s, '999') FROM tn_fs ORDER BY id;
SELECT 'tp_fs WHERE', arraySort(groupArray(id)) FROM tp_fs WHERE hasAnyTokens(s, '999') SETTINGS use_skip_indexes = 0;
SELECT 'tn_fs WHERE', arraySort(groupArray(id)) FROM tn_fs WHERE hasAnyTokens(s, '999') SETTINGS use_skip_indexes = 0;

SELECT '6. Map(String, Nullable(String)) element, index on mapValues.';

CREATE TABLE tp_map (id UInt32, m Map(String, Nullable(String)), INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_map (id UInt32, m Map(String, Nullable(String)), INDEX tix mapValues(m) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_map VALUES (1, map()), (2, map('k', 'Hello')), (3, map('k', NULL)), (4, map('j', 'x'));
INSERT INTO tn_map VALUES (1, map()), (2, map('k', 'Hello')), (3, map('k', NULL)), (4, map('j', 'x'));
SELECT 'tp_map', id, hasAnyTokens(m['k'], 'hello') FROM tp_map ORDER BY id;
SELECT 'tn_map', id, hasAnyTokens(m['k'], 'hello') FROM tn_map ORDER BY id;
SELECT 'tp_map NOT', arraySort(groupArray(id)) FROM tp_map WHERE NOT hasAnyTokens(m['k'], 'hello') SETTINGS use_skip_indexes = 0;
SELECT 'tn_map NOT', arraySort(groupArray(id)) FROM tn_map WHERE NOT hasAnyTokens(m['k'], 'hello') SETTINGS use_skip_indexes = 0;

SELECT '7. Direct read with a part where the index is not materialized.';

CREATE TABLE tp_partial (id UInt32, s Nullable(String)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
SYSTEM STOP MERGES tp_partial;
INSERT INTO tp_partial VALUES (1, NULL), (2, 'Hello world'), (3, 'foo');
ALTER TABLE tp_partial ADD INDEX tix s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s));
INSERT INTO tp_partial SETTINGS materialize_skip_indexes_on_insert = 1 VALUES (11, NULL), (12, 'Hello'), (13, 'bar');
SELECT 'tp_partial', arraySort(groupArray(id)) FROM tp_partial WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'tp_partial direct read', countIf(position(explain, '__text_index_tix_hasAnyTokens_') > 0) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tp_partial WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1);

SELECT '8. Array(Nullable(FixedString)), Array(LowCardinality(Nullable(FixedString))) and Map values: a NULL element must not produce tokens.';

CREATE TABLE tp_afs (id UInt32, a Array(Nullable(FixedString(6))), INDEX tix a TYPE text(tokenizer = ngrams(3), postprocessor = if(match(a, '[a-z]'), a, '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_afs (id UInt32, a Array(Nullable(FixedString(6))), INDEX tix a TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_afs VALUES (1, [NULL]), (2, ['hello']), (3, [NULL, 'hello']), (4, []), (5, ['12345']);
INSERT INTO tn_afs VALUES (1, [NULL]), (2, ['hello']), (3, [NULL, 'hello']), (4, []), (5, ['12345']);
SELECT 'tp_afs', id, hasAnyTokens(a, '999'), hasAllTokens(a, '999') FROM tp_afs ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'tn_afs', id, hasAnyTokens(a, '999'), hasAllTokens(a, '999') FROM tn_afs ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'tp_afs WHERE', arraySort(groupArray(id)) FROM tp_afs WHERE hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 0;
SELECT 'tn_afs WHERE', arraySort(groupArray(id)) FROM tn_afs WHERE hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 0;
SELECT 'tp_afs index', arraySort(groupArray(id)) FROM tp_afs WHERE hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'tp_afs direct read', countIf(position(explain, '__text_index_tix_hasAnyTokens_') > 0) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tp_afs WHERE hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1);

CREATE TABLE tp_mfs (id UInt32, m Map(String, Nullable(FixedString(6))), INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_mfs (id UInt32, m Map(String, Nullable(FixedString(6))), INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_mfs VALUES (1, map()), (2, map('k', 'hello')), (3, map('k', NULL)), (4, map('j', NULL, 'k', 'hello')), (5, map('k', '12345'));
INSERT INTO tn_mfs VALUES (1, map()), (2, map('k', 'hello')), (3, map('k', NULL)), (4, map('j', NULL, 'k', 'hello')), (5, map('k', '12345'));
SELECT 'tp_mfs WHERE', arraySort(groupArray(id)) FROM tp_mfs WHERE hasAnyTokens(mapValues(m), '999') SETTINGS use_skip_indexes = 0;
SELECT 'tn_mfs WHERE', arraySort(groupArray(id)) FROM tn_mfs WHERE hasAnyTokens(mapValues(m), '999') SETTINGS use_skip_indexes = 0;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tp_alc (id UInt32, a Array(LowCardinality(Nullable(FixedString(6)))), INDEX tix a TYPE text(tokenizer = ngrams(3), postprocessor = if(match(a, '[a-z]'), a, '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_alc (id UInt32, a Array(LowCardinality(Nullable(FixedString(6)))), INDEX tix a TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_alc VALUES (1, [NULL]), (2, ['hello']), (3, [NULL, 'hello']), (4, []), (5, ['12345']);
INSERT INTO tn_alc VALUES (1, [NULL]), (2, ['hello']), (3, [NULL, 'hello']), (4, []), (5, ['12345']);
SELECT 'tp_alc WHERE', arraySort(groupArray(id)) FROM tp_alc WHERE hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 0;
SELECT 'tn_alc WHERE', arraySort(groupArray(id)) FROM tn_alc WHERE hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 0;

CREATE TABLE tp_apre (id UInt32, a Array(FixedString(6)), INDEX tix a TYPE text(tokenizer = ngrams(3), preprocessor = nullIf(a, '12345'), postprocessor = if(match(a, '[a-z]'), a, '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_apre (id UInt32, a Array(FixedString(6)), INDEX tix a TYPE text(tokenizer = ngrams(3), preprocessor = nullIf(a, '12345')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_apre VALUES (1, ['12345']), (2, ['hello']), (3, ['67890']);
INSERT INTO tn_apre VALUES (1, ['12345']), (2, ['hello']), (3, ['67890']);
-- The preprocessor makes the element of row 1 NULL, which the index build skips.
SELECT 'tp_apre NOT', arraySort(groupArray(id)) FROM tp_apre WHERE NOT hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'tn_apre NOT', arraySort(groupArray(id)) FROM tn_apre WHERE NOT hasAnyTokens(a, '999') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

SELECT '9. Nullable(FixedString) whose postprocessor rejects the nested default of NULL: a NULL value is not tokenized.';

CREATE TABLE tp_fs_num (id UInt32, s Nullable(FixedString(6)), INDEX tix s TYPE text(tokenizer = ngrams(6), postprocessor = toString(toUInt64(s) % 10)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_fs_num (id UInt32, s Nullable(FixedString(6)), INDEX tix s TYPE text(tokenizer = ngrams(6)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tp_lcfs_num (id UInt32, s LowCardinality(Nullable(FixedString(6))), INDEX tix s TYPE text(tokenizer = ngrams(6), postprocessor = toString(toUInt64(s) % 10)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tn_lcfs_num (id UInt32, s LowCardinality(Nullable(FixedString(6))), INDEX tix s TYPE text(tokenizer = ngrams(6)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp_fs_num VALUES (1, NULL), (2, '123456');
INSERT INTO tn_fs_num VALUES (1, NULL), (2, '123456');
INSERT INTO tp_lcfs_num VALUES (1, NULL), (2, '123456');
INSERT INTO tn_lcfs_num VALUES (1, NULL), (2, '123456');
-- '999996' and '123456' both map to '6'. Without short-circuit evaluation, tokens of a NULL reaching the postprocessor throw.
SELECT 'tp_fs_num', id, hasAnyTokens(s, '999996') FROM tp_fs_num ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0, short_circuit_function_evaluation = 'disable';
SELECT 'tn_fs_num', id, hasAnyTokens(s, '999996') FROM tn_fs_num ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0, short_circuit_function_evaluation = 'disable';
SELECT 'tp_lcfs_num', id, hasAnyTokens(s, '999996') FROM tp_lcfs_num ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0, short_circuit_function_evaluation = 'disable';
SELECT 'tn_lcfs_num', id, hasAnyTokens(s, '999996') FROM tn_lcfs_num ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0, short_circuit_function_evaluation = 'disable';

DROP TABLE tp;
DROP TABLE tn;
DROP TABLE tp_lower;
DROP TABLE tn_lower;
DROP TABLE tp_ifnull;
DROP TABLE tn_ifnull;
DROP TABLE tp_nullif;
DROP TABLE tn_nullif;
DROP TABLE tp_nullif_str;
DROP TABLE tp_nullif_fs;
DROP TABLE tp_lc;
DROP TABLE tn_lc;
DROP TABLE tp_fs;
DROP TABLE tn_fs;
DROP TABLE tp_map;
DROP TABLE tn_map;
DROP TABLE tp_partial;
DROP TABLE tp_afs;
DROP TABLE tn_afs;
DROP TABLE tp_mfs;
DROP TABLE tn_mfs;
DROP TABLE tp_alc;
DROP TABLE tn_alc;
DROP TABLE tp_apre;
DROP TABLE tn_apre;
DROP TABLE tp_fs_num;
DROP TABLE tn_fs_num;
DROP TABLE tp_lcfs_num;
DROP TABLE tn_lcfs_num;
