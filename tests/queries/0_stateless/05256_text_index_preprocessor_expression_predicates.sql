-- A predicate on the preprocessor expression of a text index, e.g. `hasToken(lower(s), 'x')` for `preprocessor = lower(s)`,
-- uses the index like an index without a preprocessor built on that expression: the needle is not preprocessed again.

SET explain_query_plan_default = 'legacy';

DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'Error in Module'), (2, 'all fine'), (3, 'CONNECTION refused'), (4, 'error: disk full'), (5, 'Disk ERROR'), (6, 'charging and Charged');

SELECT '-- Predicates on lower(s) return the same rows with and without the index';

CREATE VIEW v AS
          SELECT 'hasToken' AS predicate, arraySort(groupArray(id)) AS ids FROM tab WHERE hasToken(lower(s), 'error')
UNION ALL SELECT 'hasToken, mixed case', arraySort(groupArray(id)) FROM tab WHERE hasToken(lower(s), 'Error')
UNION ALL SELECT 'NOT hasToken', arraySort(groupArray(id)) FROM tab WHERE NOT hasToken(lower(s), 'error')
UNION ALL SELECT 'hasAnyTokens', arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(lower(s), 'disk refused')
UNION ALL SELECT 'hasAnyTokens, tokenizer argument', arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(lower(s), 'disk refused', 'splitByNonAlpha')
UNION ALL SELECT 'hasAnyTokens, array, mixed case', arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(lower(s), ['Disk', 'refused'])
UNION ALL SELECT 'hasAllTokens', arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(lower(s), 'disk error')
UNION ALL SELECT 'hasAllTokens, mixed case', arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(lower(s), 'Disk error')
UNION ALL SELECT 'hasPhrase', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(lower(s), 'disk full')
UNION ALL SELECT 'hasPhrase, mixed case', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(lower(s), 'Disk full')
UNION ALL SELECT 'like', arraySort(groupArray(id)) FROM tab WHERE lower(s) LIKE '%rror%'
UNION ALL SELECT 'like, mixed case', arraySort(groupArray(id)) FROM tab WHERE lower(s) LIKE '%RROR%'
UNION ALL SELECT 'ilike', arraySort(groupArray(id)) FROM tab WHERE lower(s) ILIKE '%CONNECT%'
UNION ALL SELECT 'startsWith', arraySort(groupArray(id)) FROM tab WHERE startsWith(lower(s), 'erro')
UNION ALL SELECT 'endsWith', arraySort(groupArray(id)) FROM tab WHERE endsWith(lower(s), 'full')
UNION ALL SELECT 'match', arraySort(groupArray(id)) FROM tab WHERE match(lower(s), 'charg(ed|ing)')
UNION ALL SELECT 'equals', arraySort(groupArray(id)) FROM tab WHERE lower(s) = 'disk error'
UNION ALL SELECT 'equals, mixed case', arraySort(groupArray(id)) FROM tab WHERE lower(s) = 'Disk error'
UNION ALL SELECT 'in', arraySort(groupArray(id)) FROM tab WHERE lower(s) IN ('all fine', 'nothing');

SELECT * FROM v ORDER BY predicate SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT * FROM v ORDER BY predicate SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT * FROM v ORDER BY predicate SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 1;
SELECT * FROM v ORDER BY predicate SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT count() FROM tab WHERE hasToken(lower(s), 'error');
SELECT count() FROM tab WHERE hasToken(lower(s), 'Error');

SELECT '-- The needle is not preprocessed in the SELECT list either';
SELECT id, hasToken(lower(s), 'Error'), hasAnyTokens(lower(s), 'Disk'), hasAnyTokens(lower(s), 'disk') FROM tab WHERE hasToken(lower(s), 'disk') ORDER BY id;

SELECT '-- The index prunes granules';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(lower(s), 'error')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(lower(s), 'Error')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE lower(s) LIKE '%rror%') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- Direct read replaces the predicate';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(lower(s), 'error') SETTINGS query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- A predicate on the column itself is unchanged';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s, 'Error') SETTINGS force_data_skipping_indices = 'idx';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(s, 'DISK Refused') SETTINGS force_data_skipping_indices = 'idx';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s, 'disk') AND hasToken(lower(s), 'full') SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- A different expression does not use the index';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(lowerUTF8(s), 'error');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(lowerUTF8(s), 'error') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(upper(s), 'ERROR') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(lower(s), 'disk', 'ngrams(3)') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP VIEW v;
DROP TABLE tab;

SELECT '-- upper preprocessor';

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(s))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'Error in Module'), (2, 'all fine'), (3, 'CONNECTION refused'), (4, 'error: disk full'), (5, 'Disk ERROR'), (6, 'charging and Charged');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(upper(s), 'ERROR');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(upper(s), 'ERROR') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(upper(s), 'error');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(upper(s), 'error') SETTINGS use_skip_indexes = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(upper(s), 'ERROR')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(lower(s), 'error') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;

SELECT '-- An index with a postprocessor is not used';

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(s), postprocessor = if(s = 'and', '', s))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'Error in Module'), (2, 'all fine'), (3, 'CONNECTION refused'), (4, 'error: disk full'), (5, 'Disk ERROR'), (6, 'charging and Charged');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(lower(s), 'error');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(lower(s), 'error') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;
