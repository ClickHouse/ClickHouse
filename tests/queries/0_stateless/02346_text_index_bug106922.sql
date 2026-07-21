-- Tags: long
-- Tests https://github.com/ClickHouse/ClickHouse/issues/106922
-- A negated text-search predicate over a Nullable indexed column must not return rows whose
-- indexed value is NULL: f(NULL, ...) is NULL, NOT NULL is NULL, so a NULL row is filtered out.
-- The direct-read-from-text-index optimization materializes a non-Nullable 0/1 from the posting
-- list (NULL row has no tokens -> 0). A plain CAST to Nullable adds an all-false null map, so a
-- NULL row reads as a genuine 0 and NOT wrongly keeps it. The fix wraps the virtual column with
-- if(isNull(haystack), NULL, vc), re-deriving the haystack's real null map, so direct read stays
-- enabled for Nullable columns. This affects every direct-read function, not only hasToken:
-- hasAnyTokens, hasAllTokens, hasPhrase, startsWith, endsWith and the LIKE rewrites all go through
-- the same path. Each query is checked against the same query with the optimization disabled,
-- which evaluates the real function and is the reference for correct NULL semantics.

SET enable_analyzer = 1;
-- Exercise both direct-read modes (Exact replacement and Hint and(vc, real_func)) and the LIKE
-- dictionary-scan rewrite, all of which must preserve NULL semantics for a Nullable haystack.
SET query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT 'Nullable(String)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- NOT hasToken: NULL rows (2, 4) must not appear; only row 3 matches';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasAllTokens: only row 3 matches';
SELECT id FROM tab WHERE NOT hasAllTokens(str, ['hello']) ORDER BY id;
SELECT id FROM tab WHERE NOT hasAllTokens(str, ['hello']) ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasAnyTokens: only row 3 matches';
SELECT id FROM tab WHERE NOT hasAnyTokens(str, ['hello']) ORDER BY id;
SELECT id FROM tab WHERE NOT hasAnyTokens(str, ['hello']) ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasPhrase (Hint mode): rows 3 and 5 match, NULL rows excluded';
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT endsWith (Hint mode): rows 3 and 5 match, NULL rows excluded';
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id;
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT startsWith (Hint mode): only row 3, NULL rows excluded';
SELECT id FROM tab WHERE NOT startsWith(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT startsWith(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT LIKE suffix pattern (Hint mode): rows 3 and 5 match, NULL rows excluded';
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id;
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT match: only row 3, NULL rows excluded';
SELECT id FROM tab WHERE NOT match(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT match(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- positive hasPhrase still correct: only row 1';
SELECT id FROM tab WHERE hasPhrase(str, 'hello world') ORDER BY id;

SELECT '-- positive hasToken still correct: rows 1 and 5';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- hasToken IS NULL selects exactly the NULL rows 2 and 4';
SELECT id FROM tab WHERE hasToken(str, 'hello') IS NULL ORDER BY id;

SELECT '-- projection of hasToken keeps NULL for NULL rows';
SELECT id, hasToken(str, 'hello') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'Nullable(String), all-NULL part';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, NULL), (2, NULL), (3, NULL);

SELECT '-- NOT hasToken over an all-NULL part returns no rows (NOT NULL filters every row out)';
SELECT count() FROM tab WHERE NOT hasToken(str, 'hello');
SELECT count() FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'LowCardinality(Nullable(String))';

CREATE TABLE tab
(
    id  UInt32,
    str LowCardinality(Nullable(String)),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- NOT hasToken with LowCardinality(Nullable): only row 3';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Nullable(String) with preprocessor = lower(str)';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello World'), (2, NULL), (3, 'foo bar'), (4, NULL);

SELECT '-- NOT hasToken with preprocessor + Nullable: only row 3';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

-- Nullable(String) source combined with a null-producing preprocessor (nullIf(str, '')). The
-- preprocessor turns a non-NULL source value (row 2 = '') into NULL, which the source-keyed
-- if(isNull(source), ...) guard cannot see. Left enabled, direct read would read 0 for such a row
-- from a materialized part but keep the fallback predicate for an unmaterialized part, so the same
-- value gives different answers depending on per-part materialization. Direct read is therefore
-- disabled whenever the preprocessor can introduce NULLs on a Nullable haystack, and every part
-- takes the deterministic fallback: hasToken(nullIf(str,''), ...) is NULL for row 2, so NOT ... is
-- NULL and row 2 is filtered, matching the recheck path. Genuine source NULL (row 4) is filtered too.
SELECT 'Nullable(String) with null-producing preprocessor = nullIf(str, '''')';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nullIf(str, ''))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo'), (4, NULL);

SELECT '-- rows 2 (nullIf -> NULL) and 4 (source NULL) filtered; direct read matches recheck';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

-- Same null-producing preprocessor with a PARTIALLY materialized index: old rows are inserted before
-- ALTER ADD INDEX (evaluated by the fallback predicate) and new rows after (read from postings). The
-- str = '' rows in either part (id 50 old, 100050 new) must get the SAME verdict; before the fix the
-- materialized part read 0 and kept the row while the unmaterialized part filtered it. Direct read is
-- disabled here, so both parts filter it. The hint stays active via a selective needle and filler rows.
SELECT 'Nullable(String) null-producing preprocessor, partially materialized index';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab SELECT number, if(number = 50, '', concat('filler ', toString(number), ' text')) FROM numbers(200);
INSERT INTO tab VALUES (60, 'hello world here');
ALTER TABLE tab ADD INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nullIf(str, '')) GRANULARITY 1;
INSERT INTO tab SELECT number + 100000, if(number = 50, '', concat('filler ', toString(number), ' text')) FROM numbers(200);
INSERT INTO tab VALUES (100060, 'hello world here');

SELECT '-- empty-string rows (50 old, 100050 new) get the same verdict as recheck';
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') AND str = '' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') AND str = '' ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0);

DROP TABLE tab;

-- Plain (non-Nullable) String haystack with the same null-producing preprocessor, but a NULLABLE
-- needle (toNullable('hello')). The predicate result type is Nullable(UInt8) because of the needle,
-- yet the indexed HAYSTACK is a plain String, so this is out of scope for the demotion guard: direct
-- read must stay enabled. The guard keys off haystack nullability (original_haystack), not the
-- Nullable result type, so a nullable needle over a plain String source is not routed to the fallback
-- and its answer is unchanged. (Row 2 str = '' under negation is the separate pre-existing 0-vs-NULL
-- preprocessor question for non-Nullable sources, not touched here.)
SELECT 'String (non-Nullable) haystack + null-producing preprocessor + nullable needle';

CREATE TABLE tab
(
    id  UInt32,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nullIf(str, '')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo'), (4, 'world');

SELECT '-- direct read stays enabled (haystack is plain String): reads virtual column';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, toNullable('hello')) SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;

-- Nullable(String) haystack with a null-producing preprocessor (nullIf(str, '')), but a raw-string
-- sibling predicate (endsWith) whose fallback is NOT rewritten through the preprocessor. endsWith
-- evaluates the original source value on the fallback path, so a str = '' row reads 0 from postings on
-- the materialized part and evaluates endsWith('', 'world') = 0 on the unmaterialized part -- same
-- verdict, no NULL divergence. The demotion guard is therefore scoped to needApplyPreprocessor-true
-- functions (hasToken / hasAllTokens / hasAnyTokens / hasPhrase); it must NOT disable direct read for
-- endsWith. An ngrams tokenizer is used so the raw-string sibling is actually direct-readable (Hint):
-- with the broader result-type-based guard this virtual column was wrongly dropped.
SELECT 'Nullable(String) null-producing preprocessor + raw-string sibling (endsWith) keeps direct read';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = ngrams(3), preprocessor = nullIf(str, '')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab SELECT number, if(number % 777 = 0, '', concat('doc', toString(number), ' world')) FROM numbers(3000);

SELECT '-- direct read stays enabled for endsWith (raw-string sibling, not rewritten via preprocessor)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT endsWith(str, 'world') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1, use_text_index_like_evaluation_by_dictionary_scan = 1);

DROP TABLE tab;

-- Nullable(FixedString) haystack with a pure type-widening preprocessor CAST(str, 'Nullable(String)').
-- The preprocessor result type is Nullable, but CAST only widens a non-NULL input to Nullable and can
-- never turn a non-NULL row into NULL (it throws on a genuine conversion failure rather than producing
-- NULL). So there is no materialized-vs-unmaterialized NULL divergence and direct read must stay enabled,
-- even for the needApplyPreprocessor-true functions (hasToken here). The demotion must key on whether the
-- preprocessor can actually introduce a NULL from a non-NULL value, not on the declared result type being
-- Nullable. This shape (Nullable(FixedString) + CAST to Nullable(String)) is an accepted preprocessor
-- (see 04038_text_index_preprocessor_type_validation). With the broader result-type-based guard this
-- virtual column was wrongly dropped.
SELECT 'Nullable(FixedString) type-widening preprocessor CAST(str, Nullable(String)) keeps direct read';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(FixedString(16)),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(str, 'Nullable(String)')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab VALUES (1, toFixedString('hello world', 16)), (2, NULL), (3, toFixedString('foo bar', 16)), (4, NULL), (5, toFixedString('hello there', 16));

SELECT '-- NOT hasToken: NULL rows (2, 4) excluded; only row 3 matches; direct read matches recheck';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- direct read stays enabled for a pure type-widening CAST-to-Nullable preprocessor';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1, use_text_index_like_evaluation_by_dictionary_scan = 1);

DROP TABLE tab;

SELECT 'Nullable(String) with ngrams tokenizer, partially materialized index';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree ORDER BY id;

-- First part is written before the index materializes, second after, so the index is
-- partially materialized: some granules read postings directly, others re-run the function.
INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar');
SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (4, NULL), (5, 'hello there');

SELECT '-- NOT hasPhrase (ngrams, partially materialized): rows 3 and 5, NULL rows excluded';
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT endsWith (ngrams, partially materialized): rows 3 and 5, NULL rows excluded';
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id;
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT LIKE suffix pattern (ngrams, partially materialized): rows 3 and 5, NULL rows excluded';
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id;
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Map(String, Nullable(String)) with mapValues index';

-- A `mapValues(m)` index uses direct read only as a Hint (the key must still be matched), and the
-- Hint-mode virtual column is built on the same direct-read path, so the null-map restoration must
-- cover it too. The bug only shows when the Hint is kept (not bypassed), so use enough rows with a
-- selective needle that the index materializes per-row posting membership. A NULL map value reads
-- as 0 there, and `NOT and(0, NULL)` wrongly keeps the row unless the virtual column is wrapped with
-- if(isNull(haystack), NULL, vc) to carry the map value's real null map.
CREATE TABLE tab
(
    id UInt32,
    m  Map(String, Nullable(String)),
    INDEX idx mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

INSERT INTO tab
SELECT number AS id,
       multiIf(number % 500 = 0, map('k', CAST(NULL AS Nullable(String))),
               number % 777 = 0, map('k', 'needle here'),
               map('k', 'common filler text')) AS m
FROM numbers(10000);

SELECT '-- NOT hasToken(m[key]): NULL map values must be excluded; count matches the non-direct-read path';
SELECT count() FROM tab WHERE NOT hasToken(m['k'], 'needle');
SELECT count() FROM tab WHERE NOT hasToken(m['k'], 'needle') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasPhrase(m[key]) (Hint mode): NULL map values must be excluded';
SELECT count() FROM tab WHERE NOT hasPhrase(m['k'], 'needle here');
SELECT count() FROM tab WHERE NOT hasPhrase(m['k'], 'needle here') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT LIKE(m[key]): NULL map values must be excluded';
SELECT count() FROM tab WHERE NOT (m['k'] LIKE '%needle%');
SELECT count() FROM tab WHERE NOT (m['k'] LIKE '%needle%') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- no NULL map value leaks into the negated result';
SELECT countIf(m['k'] IS NULL) FROM tab WHERE NOT hasToken(m['k'], 'needle');

SELECT '-- positive hasToken(m[key]) still uses the index and is correct';
SELECT count() FROM tab WHERE hasToken(m['k'], 'needle');

DROP TABLE tab;

SELECT 'PREWHERE';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- NOT hasToken in PREWHERE: only row 3';
SELECT id FROM tab PREWHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab PREWHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Direct read stays enabled for a Nullable haystack';

-- The fix must preserve NULL semantics WITHOUT disabling the optimization. Assert the text-index
-- virtual column is still created (and wrapped with isNull(haystack)) for a Nullable column, so a
-- regression that falls back to a full scan for every Nullable predicate is caught.
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- virtual column is read directly and guarded by isNull(haystack)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column,
       countIf(explain LIKE '%isNull(str)%' OR explain LIKE '%str IS NULL%') > 0 AS guarded_by_isnull
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;

SELECT 'Null-eliminating preprocessor keeps direct read consistent across materialization';

-- A preprocessor like ifNull(str, '') / coalesce(str, '') / assumeNotNull(str) removes the source
-- nullability: the rewritten fallback predicate evaluates a source-NULL row to 0, not NULL. The
-- isNull(source) wrapper must NOT reintroduce NULL for those rows, otherwise materialized parts drop
-- the row while unmaterialized parts keep it -- a materialization-dependent result. Insert a part
-- before ADD INDEX (unmaterialized) and a part after (materialized), then MATERIALIZE, and assert the
-- answer is identical to the non-direct-read path at every step.

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');
ALTER TABLE tab ADD INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = ifNull(str, '')) GRANULARITY 1;
INSERT INTO tab VALUES (11, 'hello'), (12, NULL), (13, 'foo');

SELECT '-- ifNull: NULL source -> 0, kept under NOT; mixed materialized/unmaterialized parts agree';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

ALTER TABLE tab MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT '-- after full materialization the answer is unchanged';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;

SELECT '-- direct read stays enabled but the isNull wrap is suppressed (effective haystack is non-Nullable)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column,
       countIf(explain LIKE '%isNull(str)%' OR explain LIKE '%str IS NULL%') AS isnull_wrap_count
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;

-- coalesce and assumeNotNull remove nullability the same way.
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = coalesce(str, '')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');

SELECT '-- coalesce: source NULL kept under NOT, matches the non-direct-read path';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = assumeNotNull(str)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');

SELECT '-- assumeNotNull: source NULL kept under NOT, matches the non-direct-read path';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

-- A null-PRODUCING preprocessor (nullIf) is the opposite case: it must still disable direct read
-- (tracked by preprocessorCanIntroduceNullOnNullableHaystack), so direct read == the fallback path.
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nullIf(str, '')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo'), (4, NULL);

SELECT '-- nullIf still demotes: direct read matches the non-direct-read path';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

-- A null-REMOVING preprocessor widened back to Nullable, e.g. CAST(ifNull(str, ''), 'Nullable(String)')
-- (an accepted preprocessor shape). The declared result type is Nullable, but ifNull strips the source
-- null map before the outer CAST widens it, so the effective haystack maps every source-NULL row to a
-- non-NULL value. removesNull() must peel the widening CAST to see the inner ifNull, so the source-keyed
-- isNull wrapper is suppressed and direct read stays enabled with a consistent (materialization-
-- independent) answer that matches the non-direct-read path.
SELECT 'Nullable(String) widened null-removing preprocessor = CAST(ifNull(str, ''''), ''Nullable(String)'')';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(ifNull(str, ''), 'Nullable(String)')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');

SELECT '-- direct read matches the non-direct-read path (source NULL kept under NOT)';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- direct read stays enabled but the isNull wrap is suppressed (effective haystack is non-Nullable)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column,
       countIf(explain LIKE '%isNull(str)%' OR explain LIKE '%str IS NULL%') AS isnull_wrap_count
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;

-- The same widening CAST around a genuinely null-PRODUCING nullIf must NOT be misclassified as
-- null-removing: peeling stops at the inner nullIf (which is still Nullable), so direct read is still
-- demoted and matches the non-direct-read path.
SELECT 'Nullable(String) widened null-producing preprocessor = CAST(nullIf(str, ''''), ''Nullable(String)'') still demotes';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(nullIf(str, ''), 'Nullable(String)')) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo'), (4, NULL);

SELECT '-- direct read matches the non-direct-read path';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

-- A null-PROPAGATING preprocessor over a widened source, e.g. lower(toNullable(str)). lower uses the
-- default Nullable handling: it maps a source-NULL row to NULL and any non-NULL row to a non-NULL
-- value, so it can never synthesize NULL from a non-NULL value -- it only propagates the source
-- nullability (semantically identical to the already-covered CAST(lower(str), 'Nullable(String)')).
-- canIntroduceNull() must therefore classify it as non-null-producing (peel the widening toNullable
-- AND recurse through the null-propagating lower), so direct read stays enabled and the virtual
-- column is still read; the outer if(isNull(str), NULL, vc) wrapper preserves the real null map.
SELECT 'Nullable(String) null-propagating preprocessor = lower(toNullable(str)) keeps direct read';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(toNullable(str))) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab VALUES (1, 'Hello'), (2, NULL), (3, 'Foo');

SELECT '-- direct read matches the non-direct-read path (source NULL kept under NOT)';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- the virtual column is still read (the optimization is NOT dropped for a null-propagating preprocessor)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;

-- A null-REMOVING preprocessor wrapped in a null-PROPAGATING outer function, e.g.
-- lower(CAST(ifNull(str, ''), 'Nullable(String)')). The inner ifNull maps every source-NULL row to a
-- non-NULL '', and lower merely propagates that (already non-NULL) value, so the effective haystack
-- is never NULL even though the outer lower node declares Nullable(String). removesNull() must peel
-- the widening cast AND recurse through the null-propagating lower to detect the inner null removal,
-- otherwise preserve_haystack_null re-adds if(isNull(str), NULL, vc) and direct read drops the
-- source-NULL row that the non-direct-read fallback keeps (NOT hasToken evaluates NULL->'' to 1).
SELECT 'Nullable(String) null-removing preprocessor wrapped in lower = lower(CAST(ifNull(str, ''''), ''Nullable(String)'')) suppresses the isNull wrap';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(CAST(ifNull(str, ''), 'Nullable(String)'))) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO tab VALUES (1, 'Hello'), (2, NULL), (3, 'Foo');

SELECT '-- direct read matches the non-direct-read path (source NULL kept under NOT)';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- direct read stays enabled but the isNull wrap is suppressed (effective haystack is non-Nullable)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column,
       countIf(explain LIKE '%isNull(str)%' OR explain LIKE '%str IS NULL%') AS isnull_wrap_count
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;
