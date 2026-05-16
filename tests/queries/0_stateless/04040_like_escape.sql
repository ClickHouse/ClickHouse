-- Test LIKE with ESCAPE clause

-- Basic: literal % matching with custom escape character
SELECT '10%' LIKE '10|%' ESCAPE '|';
SELECT '10x' LIKE '10|%' ESCAPE '|';

-- Basic: literal _ matching with custom escape character
SELECT 'a_b' LIKE 'a|_b' ESCAPE '|';
SELECT 'axb' LIKE 'a|_b' ESCAPE '|';

-- Custom escape character: #
SELECT '50%off' LIKE '50#%off' ESCAPE '#';
SELECT '50xoff' LIKE '50#%off' ESCAPE '#';

-- Backslash as escape character (equivalent to default LIKE behavior)
SELECT '10%' LIKE '10\%' ESCAPE '\\';
SELECT '10x' LIKE '10\%' ESCAPE '\\';

-- Double escape character (literal escape char in pattern)
SELECT 'a|b' LIKE 'a||b' ESCAPE '|';
SELECT 'axb' LIKE 'a||b' ESCAPE '|';

-- Wildcards still work when not escaped
SELECT 'hello' LIKE 'h%o' ESCAPE '|';
SELECT 'hello' LIKE 'h_llo' ESCAPE '|';

-- NOT LIKE with ESCAPE
SELECT '10%' NOT LIKE '10|%' ESCAPE '|';
SELECT '10x' NOT LIKE '10|%' ESCAPE '|';

-- ILIKE with ESCAPE
SELECT '10%' ILIKE '10|%' ESCAPE '|';
SELECT 'ABC' ILIKE 'a|%' ESCAPE '|';

-- ILIKE with ESCAPE where case-insensitivity actually matters
SELECT 'A%B' ILIKE 'a|%b' ESCAPE '|';

-- NOT ILIKE with ESCAPE
SELECT '10%' NOT ILIKE '10|%' ESCAPE '|';

-- Backslash is literal when custom escape is used
SELECT 'a\b' LIKE 'a\b' ESCAPE '|';
SELECT 'a\b' LIKE 'a|%b' ESCAPE '|';

-- Pattern with mixed escaped and unescaped wildcards
SELECT 'test%value' LIKE 'test|%%' ESCAPE '|';
SELECT 'test%' LIKE 'test|%' ESCAPE '|';

-- Functional syntax (3-argument form)
SELECT like('10%', '10|%', '|');
SELECT notLike('10%', '10|%', '|');
SELECT ilike('A%B', 'a|%b', '|');
SELECT notILike('A%B', 'a|%b', '|');

-- Non-constant needle with ESCAPE (column as pattern)
SELECT col LIKE '10|%' ESCAPE '|' FROM (SELECT '10%' AS col);
SELECT col LIKE pat ESCAPE '|' FROM (SELECT '10%' AS col, '10|%' AS pat);

-- ESCAPE in subquery
SELECT * FROM (SELECT '10%' AS x) WHERE x LIKE '10|%' ESCAPE '|';

-- EXPLAIN SYNTAX round-trip
SELECT formatQuery('SELECT x LIKE ''abc'' ESCAPE ''|''');
SELECT formatQuery('SELECT x NOT ILIKE ''abc'' ESCAPE ''#''');

-- Error: escape sequence at end of pattern
SELECT '10%' LIKE '10|' ESCAPE '|'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

-- Error: invalid escape sequence (only %, _ and the escape char itself are valid after escape)
SELECT '10%' LIKE '10|x' ESCAPE '|'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

-- Error: empty escape string
SELECT '10%' LIKE '10|%' ESCAPE ''; -- { serverError BAD_ARGUMENTS }

-- Error: multi-character escape string
SELECT '10%' LIKE '10|%' ESCAPE '||'; -- { serverError BAD_ARGUMENTS }

-- Error: non-ASCII single-byte escape character (e.g. binary 0xFF)
SELECT '10%' LIKE '10|%' ESCAPE unhex('FF'); -- { serverError BAD_ARGUMENTS }

-- ESCAPE '\\' must stay equivalent to default LIKE: unknown escape sequences are kept literal,
-- not rejected. Compare to plain LIKE for the same patterns.
SELECT 'a\\b' LIKE 'a\\b';
SELECT 'a\\b' LIKE 'a\\b' ESCAPE '\\';
SELECT 'a_b' LIKE 'a\_b';
SELECT 'a_b' LIKE 'a\_b' ESCAPE '\\';
