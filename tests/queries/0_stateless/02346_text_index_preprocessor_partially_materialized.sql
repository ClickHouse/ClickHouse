--- Verifies that text index preprocessor is properly applied to supported functions when the index is partially materialized.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

SELECT 'Fully materialized';

DROP TABLE IF EXISTS tab_fully;
CREATE TABLE tab_fully (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id;

ALTER TABLE tab_fully ADD INDEX idx(text) TYPE text(tokenizer = splitByString([' ', '::']), preprocessor = lower(text));

SYSTEM STOP MERGES tab_fully;

INSERT INTO tab_fully SELECT number, 'FoO::Bar' from numbers(10000);
INSERT INTO tab_fully SELECT number, 'Foo::BAR' from numbers(10000);
INSERT INTO tab_fully SELECT number, 'BAr foO' from numbers(10000);
INSERT INTO tab_fully SELECT number, 'bAr fOO' from numbers(10000);

SELECT count() FROM tab_fully WHERE hasToken(text, 'FOo');
SELECT count() FROM tab_fully WHERE hasToken(text, 'BaR');

SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'FOo');
SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'BaR');
SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'FOo::bAr');
SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'BaR fOo');

SELECT count() FROM tab_fully WHERE hasAllToken(text, 'FOo');
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'BaR');
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'FOo::bAr');
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'BaR fOo');

SELECT 'Partially materialized';

DROP TABLE IF EXISTS tab_partially;
CREATE TABLE tab_partially (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id;

INSERT INTO tab_partially SELECT number, 'FoO::Bar' from numbers(10000);
INSERT INTO tab_partially SELECT number, 'BAr foO' from numbers(10000);

ALTER TABLE tab_partially ADD INDEX idx(text) TYPE text(tokenizer = splitByString([' ', '::']), preprocessor = lower(text));

SYSTEM STOP MERGES tab_partially;

INSERT INTO tab_partially SELECT number, 'Foo::BAR' from numbers(10000);
INSERT INTO tab_partially SELECT number, 'bAr fOO' from numbers(10000);

SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'FOo');
SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'BaR');
SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'FOo::bAr');
SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'BaR fOo');

SELECT count() FROM tab_partially WHERE hasAllToken(text, 'FOo');
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'BaR');
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'FOo::bAr');
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'BaR fOo');

DROP TABLE tab_partially;
DROP TABLE tab_fully;

SELECT 'Partially materialized with a non-trivial preprocessor';

-- The preprocessor strips the suffix "ing$" from tokens.
-- Old parts (row-level scan): hasToken(replaceRegexpAll('running','ing$',''), replaceRegexpAll('running','ing$',''))
--   = hasToken('runn', 'runn') = 1.
-- New parts: index stores 'runn' (preprocessed from 'running'); lookup key is also 'runn'.
-- Both parts match consistently → count is 2 for every query.

DROP TABLE IF EXISTS tab_preprocessed;
CREATE TABLE tab_preprocessed (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab_preprocessed;

INSERT INTO tab_preprocessed VALUES (1, 'running'), (2, 'cat');  -- old parts: no index

ALTER TABLE tab_preprocessed ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = replaceRegexpAll(val, 'ing$', ''));

INSERT INTO tab_preprocessed VALUES (3, 'running'), (4, 'cat');  -- new parts: with index

-- 'running' → preprocessor → 'runn'. Both old and new parts match: total 2.
SELECT count() FROM tab_preprocessed WHERE hasToken(val, 'running');  -- 2
-- 'cat' is unchanged by the preprocessor. Both parts match: total 2.
SELECT count() FROM tab_preprocessed WHERE hasToken(val, 'cat');      -- 2
SELECT count() FROM tab_preprocessed WHERE hasToken(val, 'xyz');      -- 0

SYSTEM START MERGES tab_preprocessed;
DROP TABLE tab_preprocessed;
