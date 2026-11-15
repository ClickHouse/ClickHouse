-- Tests for obfuscateQuery/obfuscateQueryWithSeed covering literal variant paths not
-- exercised by existing tests (03755, 04146). Like those tests, the checks assert properties
-- of the obfuscated text, never an exact pseudorandom output.
--
-- Previously uncovered paths: when the input query contains hex (0x) or binary (0b)
-- number literals, a string literal with UTF-8 multi-byte characters, SQL line or
-- block comments, or identifiers whose first letter is uppercase, the following code
-- paths in obfuscateQueries.cpp were never entered during CI runs:
--
--   src/Parsers/obfuscateQueries.cpp:788-853   hex (0x/0X) and binary (0b/0B) number
--       literal prefix detection and digit-by-digit obfuscation in obfuscateLiteral,
--       including hex-float fractional (.hex) and binary-exponent (p/P) sub-paths
--   src/Parsers/obfuscateQueries.cpp:736-737   first_caps (leading uppercase letter)
--       and all_caps (all letters uppercase) identifier case-style matching in
--       obfuscateIdentifier — previous tests only used lowercase identifiers
--   src/Parsers/obfuscateQueries.cpp:1125-1159 UTF-8 continuation bytes (0x80-0xBF)
--       and 2-byte (0xC0-0xDF), 3-byte (0xE0-0xEF), and 4-byte (0xF0+) sequence-start
--       bytes in string literal content in obfuscateLiteral
--   src/Parsers/obfuscateQueries.cpp:1222-1228 SQL comment token (both line and block
--       comments) in the obfuscateQuery main token-dispatch loop

-- The checks below assert the stable contract of the obfuscator, not the pseudorandom
-- replacement words and digits: the obfuscated query keeps the shape of the literal (prefix,
-- number of digits, fraction and exponent structure), the case style of identifiers, and the
-- token boundaries around comments. Exact outputs would change with the obfuscator's
-- dictionary or PRNG without any product regression.

-- Hex integer number literals: obfuscateLiteral is called for each Number token; when the
-- token starts with '0x' or '0X', the hex-prefix branch (lines 788-853) obfuscates each hex
-- digit while preserving the '0x' prefix and the number of digits.
SELECT match(obfuscateQueryWithSeed('SELECT 0xFF FROM t', 1), '^SELECT 0x[0-9A-Fa-f]{2} FROM [A-Za-z_]+$') AS hex_int_2_digits;
SELECT match(obfuscateQueryWithSeed('SELECT 0x1A2B FROM t', 1), '^SELECT 0x[0-9A-Fa-f]{4} FROM [A-Za-z_]+$') AS hex_int_4_digits;

-- Hex floating-point literals (0x<int>.<frac>p<exp>): the fractional part (lines
-- 813-818) and the binary-exponent part including an optional sign (lines 821-840) keep
-- their structure.
SELECT match(obfuscateQueryWithSeed('SELECT 0x1.8p2 FROM t', 1), '^SELECT 0x[0-9A-Fa-f]\\.[0-9A-Fa-f]p[0-9]+ FROM [A-Za-z_]+$') AS hex_float;
SELECT match(obfuscateQueryWithSeed('SELECT 0x1.0p-3 FROM t', 1), '^SELECT 0x[0-9A-Fa-f]\\.[0-9A-Fa-f]p-[0-9]+ FROM [A-Za-z_]+$') AS hex_float_negative_exponent;

-- Binary number literals: when the token starts with '0b' or '0B', the same branch keeps the
-- prefix with its case and the number of binary digits.
SELECT match(obfuscateQueryWithSeed('SELECT 0b1010 FROM t', 1), '^SELECT 0b[01]{4} FROM [A-Za-z_]+$') AS binary_lowercase_prefix;
SELECT match(obfuscateQueryWithSeed('SELECT 0B11001 FROM t', 1), '^SELECT 0B[01]{5} FROM [A-Za-z_]+$') AS binary_uppercase_prefix;

-- Uppercase identifiers (first_caps / all_caps paths in obfuscateIdentifier): a CamelCase
-- identifier stays CamelCase (first_caps, line 736) and an ALL-CAPS identifier stays
-- ALL-CAPS (all_caps, line 737). Prior tests only used lowercase identifiers.
SELECT match(obfuscateQueryWithSeed('SELECT UserID FROM events', 1), '^SELECT [A-Z][A-Za-z]*[a-z][A-Za-z]* FROM [a-z_]+$') AS first_caps_identifier;
SELECT match(obfuscateQueryWithSeed('SELECT ID FROM t', 1), '^SELECT [A-Z]+ FROM [A-Za-z_]+$') AS all_caps_identifier;

-- UTF-8 multi-byte characters in string literal content: obfuscateLiteral processes
-- the string byte-by-byte; bytes 0x80-0xBF are UTF-8 continuation bytes (lines
-- 1125-1135); bytes 0xC0-0xDF start 2-byte sequences (lines 1142-1146); bytes
-- 0xE0-0xEF start 3-byte sequences (lines 1147-1151); bytes 0xF0+ start 4-byte
-- sequences (lines 1152-1156).  We check non-emptiness to avoid non-ASCII bytes
-- in the reference file.
-- 2-byte sequences (Cyrillic, U+0400-U+04FF): continuation + 2-byte start bytes
SELECT length(obfuscateQueryWithSeed($q$SELECT 'привет' FROM t$q$, 1)) > 0 AS utf8_2byte;
-- 3-byte sequences (CJK, U+4E00+): exercises the 0xE0-0xEF branch
SELECT length(obfuscateQueryWithSeed($q$SELECT '中文' FROM t$q$, 1)) > 0 AS utf8_3byte;
-- 4-byte sequences (emoji, U+1F600): exercises the 0xF0+ branch
SELECT length(obfuscateQueryWithSeed($q$SELECT '😀' FROM t$q$, 1)) > 0 AS utf8_4byte;

-- SQL comment tokens: in the obfuscateQuery main loop, when a Comment token is
-- encountered it is replaced by a single space (lines 1222-1228) so that adjacent
-- tokens cannot merge (e.g. ORDER/**/BY must not become ORDERBY). The comment text is
-- gone and the tokens around it stay separated.
SELECT match(o, '^SELECT\\s+1 FROM [A-Za-z_]+$') AND o NOT LIKE '%comment%' AS line_comment_removed
FROM (SELECT obfuscateQueryWithSeed('SELECT -- line comment
1 FROM t', 1) AS o);
SELECT match(o, '^SELECT\\s+1 FROM [A-Za-z_]+$') AND o NOT LIKE '%comment%' AS block_comment_removed
FROM (SELECT obfuscateQueryWithSeed('SELECT /* block comment */ 1 FROM t', 1) AS o);
