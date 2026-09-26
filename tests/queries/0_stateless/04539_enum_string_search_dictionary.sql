-- Tests the Enum dictionary optimization of string search functions (issue #73114):
-- for an Enum haystack and a constant needle, the search runs over the distinct enum names only
-- and the results are mapped back per row. This must produce exactly the same results as running
-- the search over the equivalent String column (toString(c)).

DROP TABLE IF EXISTS t_es8;
DROP TABLE IF EXISTS t_es16_narrow;
DROP TABLE IF EXISTS t_es16_wide;
DROP TABLE IF EXISTS t_es16_wide_big;
DROP TABLE IF EXISTS t_es8_few;

-- Enum8 with names of different sizes (including empty and one with spaces) and negative/zero/positive values.
CREATE TABLE t_es8 (c Enum8('' = -128, 'a' = -5, 'A' = 0, 'AB' = 1, 'aBc' = 2, 'ABCD' = 3, 'xAyz' = 100, 'Foo A Bar' = 127), s UInt8)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_es8
SELECT ['', 'a', 'A', 'AB', 'aBc', 'ABCD', 'xAyz', 'Foo A Bar'][(number % 8) + 1], number % 4
FROM numbers(1000);

-- Enum16 with a narrow value span (values in [-4, 1000], span 1005) and many more rows than the span:
-- the dense lookup array is small relative to the number of rows, so the transform (fast) path is taken.
CREATE TABLE t_es16_narrow (c Enum16('' = -4, 'a' = -1, 'A' = 0, 'AB' = 7, 'aBc' = 30, 'ABCD' = 100, 'xAyz' = 500, 'Foo A Bar' = 1000))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_es16_narrow
SELECT ['', 'a', 'A', 'AB', 'aBc', 'ABCD', 'xAyz', 'Foo A Bar'][(number % 8) + 1]
FROM numbers(2000);

-- Enum16 spanning a wide, sparse Int16 range (values in [-30000, 30000], span 60001 for 8 names). The
-- transform path zero-fills a dense array of `max_value - min_value + 1` slots, so it is worth taking
-- only when a block has enough rows to amortize that fill (the span guard requires `span <= rows`).
-- With a small block (fewer rows than the span) the optimization is skipped and the unchanged
-- `castColumn` path is used; with a large block the transform path is taken. Either way the result
-- must equal `toString(...)`. See the span guard in `FunctionsStringSearch.h`.
CREATE TABLE t_es16_wide (c Enum16('' = -30000, 'a' = -1, 'A' = 0, 'AB' = 7, 'aBc' = 300, 'ABCD' = 1000, 'xAyz' = 5000, 'Foo A Bar' = 30000))
ENGINE = MergeTree ORDER BY tuple();
-- 1000 rows < span 60001, so every block keeps the `castColumn` path (regardless of block size).
INSERT INTO t_es16_wide
SELECT ['', 'a', 'A', 'AB', 'aBc', 'ABCD', 'xAyz', 'Foo A Bar'][(number % 8) + 1]
FROM numbers(1000);

CREATE TABLE t_es16_wide_big (c Enum16('' = -30000, 'a' = -1, 'A' = 0, 'AB' = 7, 'aBc' = 300, 'ABCD' = 1000, 'xAyz' = 5000, 'Foo A Bar' = 30000))
ENGINE = MergeTree ORDER BY tuple();
-- 100000 rows > span 60001; the queries below force a single large block so the transform path is taken.
INSERT INTO t_es16_wide_big
SELECT ['', 'a', 'A', 'AB', 'aBc', 'ABCD', 'xAyz', 'Foo A Bar'][(number % 8) + 1]
FROM numbers(100000);

-- Fewer rows than distinct enum names: the optimization is not applied (non-transform path).
CREATE TABLE t_es8_few (c Enum8('' = -128, 'a' = -5, 'A' = 0, 'AB' = 1, 'aBc' = 2, 'ABCD' = 3, 'xAyz' = 100, 'Foo A Bar' = 127))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_es8_few SELECT ['A', 'aBc', ''][(number % 3) + 1] FROM numbers(3);

SELECT 'Enum8 transform path';
SELECT 'like', sum(like(c, '%A%') != like(toString(c), '%A%')) FROM t_es8;
SELECT 'like ESCAPE', sum((c LIKE 'A%' ESCAPE '!') != (toString(c) LIKE 'A%' ESCAPE '!')) FROM t_es8;
SELECT 'notLike', sum(notLike(c, '%A%') != notLike(toString(c), '%A%')) FROM t_es8;
SELECT 'ilike', sum(ilike(c, '%a%') != ilike(toString(c), '%a%')) FROM t_es8;
SELECT 'match', sum(match(c, 'A') != match(toString(c), 'A')) FROM t_es8;
SELECT 'position', sum(position(c, 'A') != position(toString(c), 'A')) FROM t_es8;
SELECT 'positionCaseInsensitive', sum(positionCaseInsensitive(c, 'a') != positionCaseInsensitive(toString(c), 'a')) FROM t_es8;
SELECT 'positionUTF8', sum(positionUTF8(c, 'A') != positionUTF8(toString(c), 'A')) FROM t_es8;
SELECT 'countSubstrings', sum(countSubstrings(c, 'A') != countSubstrings(toString(c), 'A')) FROM t_es8;
SELECT 'countSubstringsCaseInsensitive', sum(countSubstringsCaseInsensitive(c, 'a') != countSubstringsCaseInsensitive(toString(c), 'a')) FROM t_es8;
SELECT 'hasToken', sum(hasToken(c, 'A') != hasToken(toString(c), 'A')) FROM t_es8;
SELECT 'hasTokenCaseInsensitive', sum(hasTokenCaseInsensitive(c, 'a') != hasTokenCaseInsensitive(toString(c), 'a')) FROM t_es8;
-- *OrNull variant exercises the Null execution error policy (the null map is mapped back too).
SELECT 'hasTokenOrNull', sum(hasTokenOrNull(c, 'A') != hasTokenOrNull(toString(c), 'A')) FROM t_es8;

SELECT 'Enum8 fallback (non-const needle)';
SELECT 'position non-const needle', sum(position(c, substring(toString(c), 1, 1)) != position(toString(c), substring(toString(c), 1, 1))) FROM t_es8;
SELECT 'Enum8 fallback (per-row start position)';
SELECT 'position per-row start', sum(position(c, 'A', s + 1) != position(toString(c), 'A', s + 1)) FROM t_es8;
SELECT 'position const start', sum(position(c, 'A', 1) != position(toString(c), 'A', 1)) FROM t_es8;

SELECT 'Enum16 transform path (narrow range)';
SELECT 'like', sum(like(c, '%A%') != like(toString(c), '%A%')) FROM t_es16_narrow SETTINGS max_block_size = 65536;
SELECT 'position', sum(position(c, 'A') != position(toString(c), 'A')) FROM t_es16_narrow SETTINGS max_block_size = 65536;
SELECT 'match', sum(match(c, 'A') != match(toString(c), 'A')) FROM t_es16_narrow SETTINGS max_block_size = 65536;
SELECT 'countSubstrings', sum(countSubstrings(c, 'A') != countSubstrings(toString(c), 'A')) FROM t_es16_narrow SETTINGS max_block_size = 65536;

-- Wide sparse Enum16, small block: the span guard keeps the `castColumn` path; result still matches.
SELECT 'Enum16 wide sparse range, small block (castColumn path)';
SELECT 'like', sum(like(c, '%A%') != like(toString(c), '%A%')) FROM t_es16_wide;
SELECT 'position', sum(position(c, 'A') != position(toString(c), 'A')) FROM t_es16_wide;
SELECT 'match', sum(match(c, 'A') != match(toString(c), 'A')) FROM t_es16_wide;
SELECT 'countSubstrings', sum(countSubstrings(c, 'A') != countSubstrings(toString(c), 'A')) FROM t_es16_wide;

-- Wide sparse Enum16, large block: more rows than the span, so the transform path is taken.
SELECT 'Enum16 wide sparse range, large block (transform path)';
SELECT 'like', sum(like(c, '%A%') != like(toString(c), '%A%')) FROM t_es16_wide_big SETTINGS max_block_size = 200000;
SELECT 'position', sum(position(c, 'A') != position(toString(c), 'A')) FROM t_es16_wide_big SETTINGS max_block_size = 200000;
SELECT 'match', sum(match(c, 'A') != match(toString(c), 'A')) FROM t_es16_wide_big SETTINGS max_block_size = 200000;
SELECT 'countSubstrings', sum(countSubstrings(c, 'A') != countSubstrings(toString(c), 'A')) FROM t_es16_wide_big SETTINGS max_block_size = 200000;

SELECT 'Enum8 non-transform path (few rows)';
SELECT 'like', sum(like(c, '%A%') != like(toString(c), '%A%')) FROM t_es8_few;
SELECT 'position', sum(position(c, 'A') != position(toString(c), 'A')) FROM t_es8_few;

-- Undefined enum codes must be rejected in the transform path, exactly as the toString(...) path does.
-- Codes outside the declared value range, and holes inside it, can reach an Enum column through binary
-- deserialization (`SerializationEnum` inherits its binary format from `SerializationNumber`, which
-- stores the raw code without validation). The optimized path must raise `UNKNOWN_ELEMENT_OF_ENUM`
-- rather than reading out of bounds (out-of-range codes) or silently reusing a name (holes).
-- Each block below has more rows than distinct enum names, so the transform path is taken.
SELECT 'Undefined enum codes rejected';
-- Out-of-range code (5) above the maximum value.
SELECT position(x, 'A') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT like(x, '%A%') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT hasToken(x, 'A') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- Code (0) below the minimum value (negative shift).
SELECT position(x, 'A') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(0, 0, 0, 0, 0)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- Hole inside the declared range (code 2 between the declared values 1 and 3).
SELECT position(x, 'A') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''c'' = 3)', char(2, 2, 2, 2, 2)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- Undefined code mixed with valid codes in the same block.
SELECT position(x, 'A') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(1, 2, 5, 1, 2)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- Enum16 out-of-range code (value 5000, little-endian bytes 136, 19).
SELECT position(x, 'A') FROM format(RowBinary, 'x Enum16(''a'' = 1, ''A'' = 2)', char(136, 19, 136, 19, 136, 19)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

-- When a query has an undefined stored code AND an invalid needle or ESCAPE argument, the undefined
-- code must be reported first, matching the non-optimized path, where the eager cast of the Enum
-- column to String precedes any needle validation.
SELECT 'Undefined enum codes reported before needle validation';
-- `hasToken` rejects needles with separators (`BAD_ARGUMENTS`), but the undefined code wins.
SELECT hasToken(x, ' ') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- Same for the Null execution error policy.
SELECT hasTokenOrNull(x, ' ') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- An invalid ESCAPE argument is validated during query analysis (over an empty block, before any
-- data is read), so it wins over the undefined code on the non-optimized path, and must keep
-- winning on the optimized path.
SELECT x LIKE '%A%' ESCAPE '!!' FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError BAD_ARGUMENTS }
-- With a valid ESCAPE argument, the undefined code is reported.
SELECT x LIKE '%A%' ESCAPE '!' FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2)', char(5, 5, 5, 5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- The same ordering holds on the non-transform path (fewer rows than distinct enum names).
SELECT hasToken(x, ' ') FROM format(RowBinary, 'x Enum8(''a'' = 1, ''A'' = 2, ''b'' = 3)', char(5, 5)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- With only valid stored codes, the needle validation errors still fire.
SELECT hasToken(c, ' ') FROM t_es8; -- { serverError BAD_ARGUMENTS }
SELECT c LIKE '%A%' ESCAPE '!!' FROM t_es8; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_es8;
DROP TABLE t_es16_narrow;
DROP TABLE t_es16_wide;
DROP TABLE t_es16_wide_big;
DROP TABLE t_es8_few;
