-- `optimize_or_like_chain` rewrites an `OR` chain of `like`/`ilike`/`match` into
-- `multiSearchAny*` / `multiMatchAny`. Both functions (`FunctionsMultiStringSearch`) accept only a
-- plain `String` haystack and throw `ILLEGAL_TYPE_OF_ARGUMENT` for `FixedString`/`Enum`, whereas the
-- original `like`/`ilike`/`match` predicates accept those types. With the setting default-on, an `OR`
-- chain over a `FixedString` or `Enum` column must therefore keep the original branches (the
-- string-only fast paths would throw, and we do not fall back to a combined `match` alternation) —
-- otherwise a previously-working query would start to throw. The optimized result must equal the
-- unoptimized one, for both analyzers.

SET optimize_or_like_chain_min_patterns = 1;
SET optimize_or_like_chain_min_substrings = 1;

DROP TABLE IF EXISTS t_or_like_fs;
CREATE TABLE t_or_like_fs (s FixedString(8)) ENGINE = Memory;
INSERT INTO t_or_like_fs VALUES ('apple'), ('banana'), ('cherry'), ('grape'), ('melon');

-- FixedString, pure-substring chain (would otherwise become `multiSearchAny`).
SELECT count() FROM t_or_like_fs WHERE s LIKE '%app%' OR s LIKE '%ban%' OR s LIKE '%cher%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_fs WHERE s LIKE '%app%' OR s LIKE '%ban%' OR s LIKE '%cher%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_fs WHERE s LIKE '%app%' OR s LIKE '%ban%' OR s LIKE '%cher%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0;

-- FixedString, non-substring chain (would otherwise become `multiMatchAny`).
SELECT count() FROM t_or_like_fs WHERE s LIKE 'app%' OR s LIKE 'ban%' OR s LIKE 'cher%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_fs WHERE s LIKE 'app%' OR s LIKE 'ban%' OR s LIKE 'cher%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_fs WHERE s LIKE 'app%' OR s LIKE 'ban%' OR s LIKE 'cher%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0;

DROP TABLE t_or_like_fs;

DROP TABLE IF EXISTS t_or_like_enum;
CREATE TABLE t_or_like_enum (e Enum8('apple' = 1, 'banana' = 2, 'cherry' = 3, 'grape' = 4, 'melon' = 5)) ENGINE = Memory;
INSERT INTO t_or_like_enum VALUES ('apple'), ('banana'), ('cherry'), ('grape'), ('melon');

-- Enum, pure-substring chain.
SELECT count() FROM t_or_like_enum WHERE e LIKE '%a%' OR e LIKE '%e%' OR e LIKE '%rr%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_enum WHERE e LIKE '%a%' OR e LIKE '%e%' OR e LIKE '%rr%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_enum WHERE e LIKE '%a%' OR e LIKE '%e%' OR e LIKE '%rr%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0;

-- Enum, non-substring chain.
SELECT count() FROM t_or_like_enum WHERE e LIKE 'app%' OR e LIKE 'ban%' OR e LIKE 'cher%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_enum WHERE e LIKE 'app%' OR e LIKE 'ban%' OR e LIKE 'cher%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_enum WHERE e LIKE 'app%' OR e LIKE 'ban%' OR e LIKE 'cher%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0;

DROP TABLE t_or_like_enum;
