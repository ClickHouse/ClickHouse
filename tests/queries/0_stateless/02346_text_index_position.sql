-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN output below differs with parallel replicas.
-- `position(s, 'x') > 0` must use the text index like `s LIKE '%x%'`.
-- Every query must return the same rows with and without the index.

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

INSERT INTO tab VALUES
    (1, 'alpha bravo'), (2, 'charlie delta'), (3, 'alphabet soup'), (4, 'lpha centauri'), (5, 'ALPHA UPPER'),
    (6, 'sale 50%_off today'), (7, 'path c:\\temp\\file'), (8, 'foo bar baz'), (9, 'bar none'),
    (10, 'Kelvin scale'), (11, 'kelvin scale'),
    (12, 'measure'), (13, 'Measure twice'), (14, 'soup kitchen'), (15, '');

SELECT '-- Comparisons that use the index';

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

SELECT 'positionCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitiveUTF8(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'positionCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitiveUTF8(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'positionCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitiveUTF8(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'countSubstringsCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitiveUTF8(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'countSubstringsCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitiveUTF8(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'countSubstringsCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitiveUTF8(message, 'LPHA') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'absent needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'nonexistent') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'absent needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'nonexistent') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'absent needle', arraySort(groupArray(id)) FROM tab WHERE position(message, 'nonexistent') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- The row 'bar none' must not match.
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

-- A needle with 'k' uses the index.
SELECT 'case-insensitive with k', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'case-insensitive with k', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'case-insensitive with k', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- A needle with 's' uses the index.
SELECT 'case-insensitive with s', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'MEASURE') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'case-insensitive with s', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'MEASURE') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'case-insensitive with s', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'MEASURE') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'or', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR position(message, 'kitchen') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'or', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR position(message, 'kitchen') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'or', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR position(message, 'kitchen') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 0;
SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'and not', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'soup') AND NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- Same as `position != 0`.
SELECT 'not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') = 0) SETTINGS use_skip_indexes = 0;
SELECT 'not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') = 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') = 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'and like', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 AND message LIKE '%lpha%' SETTINGS use_skip_indexes = 0;
SELECT 'and like', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 AND message LIKE '%lpha%' SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'and like', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 AND message LIKE '%lpha%' SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- Comparisons that do not use the index';

SELECT 'start position', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha', 3) > 0 SETTINGS use_skip_indexes = 0;
SELECT 'start position', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha', 3) > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'start position', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha', 3) > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'empty needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'empty needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'empty needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'empty needle count', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, '') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'empty needle count', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, '') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'empty needle count', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(message, '') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'empty needle count uses no index', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE countSubstrings(message, '') > 0 SETTINGS use_skip_indexes = 1) WHERE explain LIKE '%Name: idx%';

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

-- The start position form does not use the index.
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

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION positionCaseInsensitiveUTF8(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE positionCaseInsensitiveUTF8(message, 'LPHA') > 0);

-- Both conditions use the same index read.
SELECT uniqExactIf(extract(explain, '-> (__text_index_[0-9A-Za-z_]+)'), explain LIKE '%INPUT%\_\_text\_index\_%'), countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'lpha') > 0 AND message LIKE '%lpha%');

-- The index only filters, the original check stays.
SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'foo bar baz') > 0);

-- A needle with 'k': both functions are answered by the index.
SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION positionCaseInsensitive(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE positionCaseInsensitive(message, 'KELVIN') > 0);

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE positionCaseInsensitiveUTF8(message, 'KELVIN') > 0);

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'lpha', 3) > 0);

DROP TABLE tab;

SELECT '-- The ASCII-only functions do not fold the Kelvin sign to k';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- Row 3 starts with U+212A, the Kelvin sign.
INSERT INTO tab VALUES (1, 'kelvin scale'), (2, 'KELVIN SCALE'), (3, '\xE2\x84\xAAelvin scale'), (4, 'zz');

SELECT 'kelvin sign', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'kelvin') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'kelvin sign', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'kelvin') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'kelvin sign', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'kelvin') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'kelvin sign count', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'kelvin sign count', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'kelvin sign count', arraySort(groupArray(id)) FROM tab WHERE countSubstringsCaseInsensitive(message, 'KELVIN') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE positionCaseInsensitive(message, 'kelvin') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

SELECT '-- Array tokenizer: the ASCII-only functions take a non-ASCII needle';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'CAFÉ'), (2, 'café'), (3, 'CAFé'), (4, 'zz');

-- 'É' and 'é' differ outside ASCII, so row 1 does not match.
SELECT 'non-ASCII needle', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'café') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'non-ASCII needle', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'café') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'non-ASCII needle', arraySort(groupArray(id)) FROM tab WHERE positionCaseInsensitive(message, 'café') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE positionCaseInsensitive(message, 'café') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE positionCaseInsensitiveUTF8(message, 'café') > 0);

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

-- The NULL row must stay out of every negated form.
SELECT 'not position > 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 0;
SELECT 'not position > 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'not position > 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') = 0) SETTINGS use_skip_indexes = 0;
SELECT 'not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') = 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (position(message, 'lpha') = 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'not not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (NOT (position(message, 'lpha') = 0)) SETTINGS use_skip_indexes = 0;
SELECT 'not not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (NOT (position(message, 'lpha') = 0)) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'not not position = 0', arraySort(groupArray(id)) FROM tab WHERE NOT (NOT (position(message, 'lpha') = 0)) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- The same check used twice: the NULL row must not turn into 0.
SELECT 'or not', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 0;
SELECT 'or not', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'or not', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 OR NOT (position(message, 'lpha') > 0) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'select', arraySort(groupArray((id, position(message, 'lpha') > 0))) FROM tab WHERE position(message, 'lpha') > 0 OR id = 2 SETTINGS use_skip_indexes = 0;
SELECT 'select', arraySort(groupArray((id, position(message, 'lpha') > 0))) FROM tab WHERE position(message, 'lpha') > 0 OR id = 2 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'select', arraySort(groupArray((id, position(message, 'lpha') > 0))) FROM tab WHERE position(message, 'lpha') > 0 OR id = 2 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

-- A CAST that drops Nullable must still throw on the NULL row the OR keeps.
SELECT 'cast drops nullable', arraySort(groupArray(id)) FROM tab WHERE position(CAST(message, 'String'), 'lpha') > 0 OR id = 2 SETTINGS short_circuit_function_evaluation = 'disable', use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT 'cast drops nullable', arraySort(groupArray(id)) FROM tab WHERE position(CAST(message, 'String'), 'lpha') > 0 OR id = 2 SETTINGS short_circuit_function_evaluation = 'disable', use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT 'cast drops nullable', arraySort(groupArray(id)) FROM tab WHERE position(CAST(message, 'String'), 'lpha') > 0 OR id = 2 SETTINGS short_circuit_function_evaluation = 'disable', use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT 'cast drops nullable', arraySort(groupArray(id)) FROM tab WHERE countSubstrings(CAST(message, 'String'), 'lpha') > 0 OR id = 2 SETTINGS short_circuit_function_evaluation = 'disable', use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

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

SELECT '-- A part without the index: the reader evaluates the replaced comparison';

CREATE TABLE tab
(
    id UInt32,
    message String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (1, 'alpha bravo'), (2, 'charlie delta');
ALTER TABLE tab ADD INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha);
INSERT INTO tab VALUES (3, 'alphabet soup'), (4, 'soup kitchen');

SELECT 'part without the index', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'part without the index', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'part without the index', arraySort(groupArray(id)) FROM tab WHERE position(message, 'lpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(message, 'lpha') > 0);

DROP TABLE tab;

SELECT '-- Preprocessor and postprocessor';

-- This preprocessor breaks the pattern, so the index is not used.
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceRegexpAll(message, '[^a-z ]', ''))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'xp ab qx'), (2, 'zz');

SELECT 'regexp preprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'p ab q') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'regexp preprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'p ab q') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'regexp preprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'p ab q') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, 'p ab q') > 0
) WHERE explain LIKE '%Name: idx%';

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'XP AB QX'), (2, 'xp ab qx'), (3, 'zz');

SELECT 'lower preprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'P AB Q') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'lower preprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'P AB Q') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'lower preprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'P AB Q') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, 'P AB Q') > 0
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(message))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'foo bar baz'), (2, 'bar none'), (3, 'FOO BAR BAZ'), (4, 'zz');

SELECT 'postprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'foo bar baz') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'postprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'foo bar baz') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'postprocessor', arraySort(groupArray(id)) FROM tab WHERE position(message, 'foo bar baz') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE position(message, 'foo bar baz') > 0
) WHERE explain LIKE '%Name: idx%';

DROP TABLE tab;

SELECT '-- A needle cut inside a UTF-8 character';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = asciiCJK)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'x你好y'), (3, '你 hello');

-- The last two bytes of '你'.
SELECT 'cut needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '\xBD\xA0') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'cut needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '\xBD\xA0') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'cut needle', arraySort(groupArray(id)) FROM tab WHERE position(message, '\xBD\xA0') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'whole character', arraySort(groupArray(id)) FROM tab WHERE position(message, '你') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'whole character', arraySort(groupArray(id)) FROM tab WHERE position(message, '你') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'whole character', arraySort(groupArray(id)) FROM tab WHERE position(message, '你') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE tab;

SELECT '-- A map key the map does not hold';

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx(mapValues(m)) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, map('other', 'hello')), (2, map('k', 'hello')), (3, map('k', 'world'));

-- Row 1 has no key 'k', so m['k'] is ''. It holds 'hello' under another key and must not match.
SELECT 'absent key', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'absent key', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'absent key', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'absent key', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS optimize_functions_to_subcolumns = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'absent key', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS optimize_functions_to_subcolumns = 0, query_plan_direct_read_from_text_index = 1;

-- The index drops only row 3: it cannot tell the keys of rows 1 and 2 apart.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS optimize_functions_to_subcolumns = 1
) WHERE explain LIKE '%Granules: 2/3%';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE position(m['k'], 'hell') > 0 SETTINGS optimize_functions_to_subcolumns = 0
) WHERE explain LIKE '%Granules: 2/3%';

-- A needle no value holds drops every row.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE position(m['k'], 'zzzz') > 0 SETTINGS optimize_functions_to_subcolumns = 1
) WHERE explain LIKE '%Granules: 0/3%';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE position(m['k'], 'zzzz') > 0 SETTINGS optimize_functions_to_subcolumns = 0
) WHERE explain LIKE '%Granules: 0/3%';

-- The empty needle is found in '' too, so row 1 matches and the index is not used.
SELECT 'absent key, empty needle', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], '') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'absent key, empty needle', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], '') > 0 SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'absent key, empty needle', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], '') > 0 SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'absent key, empty needle', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], '') > 0 SETTINGS optimize_functions_to_subcolumns = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'absent key, empty needle', arraySort(groupArray(id)) FROM tab WHERE position(m['k'], '') > 0 SETTINGS optimize_functions_to_subcolumns = 0, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE position(m['k'], '') > 0 SETTINGS optimize_functions_to_subcolumns = 1
) WHERE explain LIKE '%Name: idx%';
SELECT count() FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE position(m['k'], '') > 0 SETTINGS optimize_functions_to_subcolumns = 0
) WHERE explain LIKE '%Name: idx%';

DROP TABLE tab;

SELECT '-- A haystack that is not valid UTF-8';

-- The ngrams, sparseGrams and asciiCJK tokenizers skip the ASCII bytes after a broken leading byte, a byte search does not.
DROP TABLE IF EXISTS tab_ngrams;
DROP TABLE IF EXISTS tab_sparse;
DROP TABLE IF EXISTS tab_cjk;
CREATE TABLE tab_ngrams (id UInt32, s String, INDEX idx(s) TYPE text(tokenizer = ngrams(3))) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tab_sparse (id UInt32, s String, INDEX idx(s) TYPE text(tokenizer = sparseGrams(3, 5), preprocessor = lower(s))) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tab_cjk (id UInt32, s String, INDEX idx(s) TYPE text(tokenizer = asciiCJK)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_ngrams VALUES (1, unhex('F0616C706861')), (2, 'beta');
INSERT INTO tab_sparse VALUES (1, unhex('F0616C706861')), (2, 'beta');
INSERT INTO tab_cjk VALUES (1, unhex('F0E4B8AD')), (2, 'beta');

SELECT 'ngrams position', arraySort(groupArray(id)) FROM tab_ngrams WHERE position(s, 'alpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'ngrams position', arraySort(groupArray(id)) FROM tab_ngrams WHERE position(s, 'alpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'ngrams position', arraySort(groupArray(id)) FROM tab_ngrams WHERE position(s, 'alpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT 'ngrams countSubstrings', arraySort(groupArray(id)) FROM tab_ngrams WHERE countSubstrings(s, 'alpha') > 0 SETTINGS use_skip_indexes = 1;
SELECT 'ngrams positionUTF8', arraySort(groupArray(id)) FROM tab_ngrams WHERE positionUTF8(s, 'alpha') > 0 SETTINGS use_skip_indexes = 1;
SELECT 'sparseGrams position', arraySort(groupArray(id)) FROM tab_sparse WHERE position(s, 'alpha') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'sparseGrams position', arraySort(groupArray(id)) FROM tab_sparse WHERE position(s, 'alpha') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'sparseGrams countSubstrings', arraySort(groupArray(id)) FROM tab_sparse WHERE countSubstrings(s, 'alpha') > 0 SETTINGS use_skip_indexes = 1;
SELECT 'asciiCJK position', arraySort(groupArray(id)) FROM tab_cjk WHERE position(s, '中') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'asciiCJK position', arraySort(groupArray(id)) FROM tab_cjk WHERE position(s, '中') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT 'asciiCJK position', arraySort(groupArray(id)) FROM tab_cjk WHERE position(s, '中') > 0 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE tab_ngrams;
DROP TABLE tab_sparse;
DROP TABLE tab_cjk;
