-- Tests for obfuscateQuery/obfuscateQueryWithSeed covering literal variant paths not
-- exercised by existing tests (03755, 04146).
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

-- Hex integer number literals: obfuscateLiteral is called for each Number token; when
-- the token starts with '0x' or '0X', the hex-prefix branch (lines 788-853) obfuscates
-- each hex digit while preserving the '0x' prefix and integer structure.
SELECT obfuscateQueryWithSeed('SELECT 0xFF FROM t', 1);
SELECT obfuscateQueryWithSeed('SELECT 0x1A2B FROM t', 1);

-- Hex floating-point literals (0x<int>.<frac>p<exp>): the fractional part (lines
-- 813-818) and binary-exponent part including optional sign (lines 821-840) sub-paths.
SELECT obfuscateQueryWithSeed('SELECT 0x1.8p2 FROM t', 1);
SELECT obfuscateQueryWithSeed('SELECT 0x1.0p-3 FROM t', 1);

-- Binary number literals: when the token starts with '0b' or '0B', the same branch
-- handles binary digit obfuscation while preserving '0b'/'0B' and the digit sequence.
SELECT obfuscateQueryWithSeed('SELECT 0b1010 FROM t', 1);
SELECT obfuscateQueryWithSeed('SELECT 0B11001 FROM t', 1);

-- Uppercase identifiers (first_caps / all_caps paths in obfuscateIdentifier):
-- obfuscateIdentifier applies case-style matching so that a CamelCase input produces
-- CamelCase output (first_caps, line 736) and an ALL-CAPS input produces ALL-CAPS
-- output (all_caps, line 737).  Prior tests only used lowercase identifiers.
SELECT obfuscateQueryWithSeed('SELECT UserID FROM events', 1);
SELECT obfuscateQueryWithSeed('SELECT ID FROM t', 1);

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
-- tokens cannot merge (e.g. ORDER/**/BY must not become ORDERBY).
SELECT obfuscateQueryWithSeed('SELECT -- line comment
1 FROM t', 1);
SELECT obfuscateQueryWithSeed('SELECT /* block comment */ 1 FROM t', 1);
