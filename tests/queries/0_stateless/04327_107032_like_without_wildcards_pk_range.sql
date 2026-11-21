-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- Tests https://github.com/ClickHouse/ClickHouse/issues/107032
-- A LIKE/NOT LIKE pattern without wildcards ('%', '_') is equivalent to an equality/inequality,
-- so KeyCondition must build an exact point range [x, x] for primary key analysis, not the wider
-- half-open prefix range [x, x_incremented) it used for patterns with a perfect prefix.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_like_pk;

CREATE TABLE t_like_pk (value String)
ENGINE = MergeTree ORDER BY value SETTINGS index_granularity = 8192;

-- 'example.com' and 'example.con' are adjacent so the old half-open range ['example.com', 'example.con')
-- swept in the 'example.com.*' rows too. The exact range must read only the 'example.com' granules.
INSERT INTO t_like_pk
SELECT arrayElement(['example.com', 'example.com.au', 'example.com.br', 'example.con', 'sample.com'], 1 + number % 5)
FROM numbers(1000000);

SELECT 'LIKE without wildcard uses an exact point range, same as equality';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_pk WHERE value LIKE 'example.com') WHERE explain LIKE '%Condition:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_pk WHERE value = 'example.com') WHERE explain LIKE '%Condition:%';

SELECT 'NOT LIKE without wildcard excludes an exact point range, same as inequality';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_pk WHERE value NOT LIKE 'example.com') WHERE explain LIKE '%Condition:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_pk WHERE value != 'example.com') WHERE explain LIKE '%Condition:%';

SELECT 'An escaped wildcard is a literal, so the pattern is still an exact match';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_pk WHERE value LIKE 'example.com\%') WHERE explain LIKE '%Condition:%';

SELECT 'A pattern with a real wildcard still uses the half-open prefix range';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_pk WHERE value LIKE 'example.com%') WHERE explain LIKE '%Condition:%';

SELECT 'Results stay correct: exact LIKE matches equality, NOT LIKE matches inequality';
SELECT count() FROM t_like_pk WHERE value LIKE 'example.com';
SELECT count() FROM t_like_pk WHERE value = 'example.com';
SELECT count() FROM t_like_pk WHERE value NOT LIKE 'example.com';
SELECT count() FROM t_like_pk WHERE value != 'example.com';

DROP TABLE t_like_pk;

-- @PedroTadim asked for a test where a text index is used. A wildcard-free LIKE tokenizes to an exact
-- token lookup, so the text skipping index prunes granules just like an equality does.
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_like_text;

CREATE TABLE t_like_text (id UInt64, value String, INDEX idx(value) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO t_like_text SELECT number, if(number = 42, 'needle', concat('hay', toString(number))) FROM numbers(1000);

SELECT 'Text index is used for a wildcard-free LIKE';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_text WHERE value LIKE 'needle') WHERE explain LIKE '%Name: idx%';
SELECT count() FROM t_like_text WHERE value LIKE 'needle';
SELECT count() FROM t_like_text WHERE value = 'needle';

DROP TABLE t_like_text;

DROP TABLE IF EXISTS t_like_trailing_escape;

CREATE TABLE t_like_trailing_escape (value String)
ENGINE = MergeTree ORDER BY value SETTINGS index_granularity = 8192;

-- 'a\' sorts before 'aa', so an exact range ['a', 'a'] would wrongly prune the 'a\' granule and the
-- query would never reach the matcher that rejects a trailing escape with CANNOT_PARSE_ESCAPE_SEQUENCE.
INSERT INTO t_like_trailing_escape VALUES ('a\\'), ('aa');

SELECT 'A trailing escape is not optimized into an exact point range';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_trailing_escape WHERE value LIKE 'a\\') WHERE explain LIKE '%Condition:%';

SELECT 'LIKE / NOT LIKE with a trailing escape raise instead of being pruned away';
SELECT count() FROM t_like_trailing_escape WHERE value LIKE 'a\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }
SELECT count() FROM t_like_trailing_escape WHERE value NOT LIKE 'a\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }
SELECT count() FROM t_like_trailing_escape WHERE value LIKE 'a\\' SETTINGS force_primary_key = 1; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE t_like_trailing_escape;

DROP TABLE IF EXISTS t_like_unknown_escape;

-- index_granularity = 1 puts 'w' and '\' + 'w' in separate granules so a wrong point range prunes the
-- wrong one and the row count changes, not just the EXPLAIN output.
CREATE TABLE t_like_unknown_escape (value String)
ENGINE = MergeTree ORDER BY value SETTINGS index_granularity = 1;

-- An unknown escape '\w' is kept as the literal two characters "\w" by the matcher (so '\w' LIKE '\w'),
-- unlike '\%'/'\_'/'\\' which fold to a single literal. The fixed prefix must be the literal "\w", not "w",
-- so the exact range must be ['\w', '\w'] and must NOT prune the 'w' granule for NOT LIKE or the '\w'
-- granule for LIKE. '\' (0x5C) sorts before 'w' (0x77) so '\w' < 'w'.
INSERT INTO t_like_unknown_escape VALUES ('w'), ('\\w');

SELECT 'An unknown escape keeps the backslash, so the exact range is the literal pattern, not the unescaped char';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_unknown_escape WHERE value LIKE '\\w') WHERE explain LIKE '%Condition:%';

SELECT 'LIKE with an unknown escape keeps the matching row under force_primary_key (matches equality)';
SELECT count() FROM t_like_unknown_escape WHERE value LIKE '\\w' SETTINGS force_primary_key = 1;
SELECT count() FROM t_like_unknown_escape WHERE value = '\\w';

SELECT 'NOT LIKE with an unknown escape keeps the non-matching row under force_primary_key (matches inequality)';
SELECT count() FROM t_like_unknown_escape WHERE value NOT LIKE '\\w' SETTINGS force_primary_key = 1;
SELECT count() FROM t_like_unknown_escape WHERE value != '\\w';

DROP TABLE t_like_unknown_escape;

DROP TABLE IF EXISTS t_like_empty;

-- The empty pattern is wildcard-free and equivalent to value = '' / value != '', so it must use an exact
-- empty-string point range, not bail out. index_granularity = 1 puts '' and 'a' in separate granules so
-- force_primary_key = 1 must succeed (the empty point range prunes a granule) and the counts must match
-- the equivalent equality / inequality.
CREATE TABLE t_like_empty (value String)
ENGINE = MergeTree ORDER BY value SETTINGS index_granularity = 1;

INSERT INTO t_like_empty VALUES (''), ('a'), ('b');

SELECT 'An empty LIKE pattern uses the exact empty-string point range, same as equality';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_empty WHERE value LIKE '') WHERE explain LIKE '%Condition:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_empty WHERE value = '') WHERE explain LIKE '%Condition:%';

SELECT 'An empty NOT LIKE pattern excludes the exact empty-string point range, same as inequality';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_empty WHERE value NOT LIKE '') WHERE explain LIKE '%Condition:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_like_empty WHERE value != '') WHERE explain LIKE '%Condition:%';

SELECT 'LIKE / NOT LIKE empty use the primary key (no INDEX_NOT_USED) and match equality / inequality';
SELECT count() FROM t_like_empty WHERE value LIKE '' SETTINGS force_primary_key = 1;
SELECT count() FROM t_like_empty WHERE value = '' SETTINGS force_primary_key = 1;
SELECT count() FROM t_like_empty WHERE value NOT LIKE '' SETTINGS force_primary_key = 1;
SELECT count() FROM t_like_empty WHERE value != '' SETTINGS force_primary_key = 1;

DROP TABLE t_like_empty;
