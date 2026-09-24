-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN output below differs with parallel replicas.
-- Tests that the text index serves `position(s, 'needle') > 0` and the comparisons equivalent to it as
-- `s LIKE '%needle%'`, and leaves alone the forms that are not a pure occurrence check. Every query is
-- run without the index, with the index, and with direct read from the index.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- Row 10 starts with U+212A KELVIN SIGN and row 12 contains U+017F LATIN SMALL LETTER LONG S: neither
-- matches an ASCII needle case-insensitively in positionCaseInsensitive.
INSERT INTO tab VALUES
    (1, 'alpha bravo'), (2, 'charlie delta'), (3, 'alphabet soup'), (4, 'lpha centauri'), (5, 'ALPHA UPPER'),
    (6, 'sale 50%_off today'), (7, 'path c:\\temp\\file'), (8, 'foo bar baz'), (9, 'bar none'),
    (10, concat(char(0xE2, 0x84, 0xAA), 'elvin scale')), (11, 'kelvin scale'),
    (12, concat('mea', char(0xC5, 0xBF), 'ure')), (13, 'Measure twice'), (14, 'soup kitchen'), (15, '');

SELECT '-- Occurrence checks';

SELECT 'position > 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'position > 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position > 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'position != 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') != 0 SETTINGS use_skip_indexes = 0;
SELECT 'position != 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') != 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position != 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') != 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'position >= 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') >= 1 SETTINGS use_skip_indexes = 0;
SELECT 'position >= 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') >= 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position >= 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') >= 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '0 < position', arraySort(groupArray(id)) FROM tab WHERE 0 < position(message, 'lpha') SETTINGS use_skip_indexes = 0;
SELECT '0 < position', arraySort(groupArray(id)) FROM tab WHERE 0 < position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT '0 < position', arraySort(groupArray(id)) FROM tab WHERE 0 < position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '0 != position', arraySort(groupArray(id)) FROM tab WHERE 0 != position(message, 'lpha') SETTINGS use_skip_indexes = 0;
SELECT '0 != position', arraySort(groupArray(id)) FROM tab WHERE 0 != position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT '0 != position', arraySort(groupArray(id)) FROM tab WHERE 0 != position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '1 <= position', arraySort(groupArray(id)) FROM tab WHERE 1 <= position(message, 'lpha') SETTINGS use_skip_indexes = 0;
SELECT '1 <= position', arraySort(groupArray(id)) FROM tab WHERE 1 <= position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT '1 <= position', arraySort(groupArray(id)) FROM tab WHERE 1 <= position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'positionUTF8', arraySort(groupArray(id)) FROM tab WHERE positionUTF8(message, 'lpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'positionUTF8', arraySort(groupArray(id)) FROM tab WHERE positionUTF8(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'positionUTF8', arraySort(groupArray(id)) FROM tab WHERE positionUTF8(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'countSubstrings', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, 'lpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'countSubstrings', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'countSubstrings', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'positionCaseInsensitive', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'positionCaseInsensitive', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'positionCaseInsensitive', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'instr', arraySort(groupArray(id)) FROM tab WHERE instr(message, 'LpHa') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'instr', arraySort(groupArray(id)) FROM tab WHERE instr(message, 'LpHa') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'instr', arraySort(groupArray(id)) FROM tab WHERE instr(message, 'LpHa') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'countSubstringsCaseInsensitive', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitive(message, 'LPHA') >= 1 SETTINGS use_skip_indexes = 0;
SELECT 'countSubstringsCaseInsensitive', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitive(message, 'LPHA') >= 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'countSubstringsCaseInsensitive', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitive(message, 'LPHA') >= 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'absent needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'nonexistent') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'absent needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'nonexistent') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'absent needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'nonexistent') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- Spaces make it a token hint on 'bar', so the row 'bar none' must still be filtered out.
SELECT 'needle with spaces', arraySort(groupArray(id)) FROM tab WHERE position(message, 'foo bar baz') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'needle with spaces', arraySort(groupArray(id)) FROM tab WHERE position(message, 'foo bar baz') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'needle with spaces', arraySort(groupArray(id)) FROM tab WHERE position(message, 'foo bar baz') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'needle with % and _', arraySort(groupArray(id)) FROM tab WHERE position(message, '50%_off') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'needle with % and _', arraySort(groupArray(id)) FROM tab WHERE position(message, '50%_off') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'needle with % and _', arraySort(groupArray(id)) FROM tab WHERE position(message, '50%_off') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'needle with backslash', arraySort(groupArray(id)) FROM tab WHERE position(message, 'c:\\temp') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'needle with backslash', arraySort(groupArray(id)) FROM tab WHERE position(message, 'c:\\temp') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'needle with backslash', arraySort(groupArray(id)) FROM tab WHERE position(message, 'c:\\temp') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- Shorter than text_index_like_min_pattern_length.
SELECT 'short needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'bar') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'short needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'bar') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'short needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'bar') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- A needle with 'k' is refused, as for ILIKE; the U+212A row must not match either way.
SELECT 'case-insensitive with k', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'case-insensitive with k', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'case-insensitive with k', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- A needle with 's' is served: the dictionary scan folds ASCII only, as positionCaseInsensitive does.
SELECT 'case-insensitive with s', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'MEASURE') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'case-insensitive with s', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'MEASURE') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'case-insensitive with s', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'MEASURE') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'or', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR position(message, 'kitchen') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'or', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR position(message, 'kitchen') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'or', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR position(message, 'kitchen') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 0;
SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- Not occurrence checks';

SELECT 'start position', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha', 3) > 0 SETTINGS use_skip_indexes = 0;
SELECT 'start position', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha', 3) > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'start position', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha', 3) > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'empty needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'empty needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'empty needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'position > 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 1 SETTINGS use_skip_indexes = 0;
SELECT 'position > 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position > 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '0 > position', arraySort(groupArray(id)) FROM tab WHERE 0 > position(message, 'lpha') SETTINGS use_skip_indexes = 0;
SELECT '0 > position', arraySort(groupArray(id)) FROM tab WHERE 0 > position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT '0 > position', arraySort(groupArray(id)) FROM tab WHERE 0 > position(message, 'lpha') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'position < 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') < 1 SETTINGS use_skip_indexes = 0;
SELECT 'position < 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') < 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position < 1', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') < 1 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'position = 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') = 0 SETTINGS use_skip_indexes = 0;
SELECT 'position = 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') = 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position = 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') = 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- Granules dropped by the index';

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, 'lpha') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE 0 < position(message, 'lpha')
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE positionCaseInsensitive(message, 'LPHA') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, 'foo bar baz') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

-- The index is not used at all for the start position form.
SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, 'lpha', 3) > 0
) WHERE explain LIKE '%Name: idx%';

SELECT '-- Direct read replaces the predicate when the index answers it exactly';

SET query_plan_direct_read_from_text_index = 1;

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'lpha') > 0);

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION positionCaseInsensitive(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE positionCaseInsensitive(message, 'LPHA') > 0);

-- A hint: the original predicate stays.
SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'foo bar baz') > 0);

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0);

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'lpha', 3) > 0);

DROP TABLE tab;

SELECT '-- Nullable column';

CREATE TABLE tab
(
    id UInt32,
    message Nullable(String),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'alpha'), (2, NULL), (3, 'beta'), (4, 'soup'), (5, 'alpha soup');

SELECT 'position > 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'position > 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'position > 0', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 0;
SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE tab;

SELECT '-- Array tokenizer: the needle is matched literally inside the whole value';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, '50%_off'), (2, '50xyoff'), (3, '50%xoff'), (4, 'c:\\temp'), (5, 'c:xtemp'), (6, 'SALE 50%_OFF');

SELECT 'needle with % and _', arraySort(groupArray(id)) FROM tab WHERE position(message, '50%_off') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'needle with % and _', arraySort(groupArray(id)) FROM tab WHERE position(message, '50%_off') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'needle with % and _', arraySort(groupArray(id)) FROM tab WHERE position(message, '50%_off') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'case-insensitive needle with % and _', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, '50%_off') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'case-insensitive needle with % and _', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, '50%_off') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'case-insensitive needle with % and _', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, '50%_off') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'needle with backslash', arraySort(groupArray(id)) FROM tab WHERE position(message, 'c:\\temp') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'needle with backslash', arraySort(groupArray(id)) FROM tab WHERE position(message, 'c:\\temp') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'needle with backslash', arraySort(groupArray(id)) FROM tab WHERE position(message, 'c:\\temp') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'countSubstrings', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, '%xoff') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'countSubstrings', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, '%xoff') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'countSubstrings', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, '%xoff') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, '50%_off') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, '50%_off') > 0);

DROP TABLE tab;
