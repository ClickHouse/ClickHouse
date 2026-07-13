-- Test for https://github.com/ClickHouse/ClickHouse/issues/106382
-- match / extract / extractAll / countMatches returned wrong results for regexps with
-- `\x` (hex) and octal escapes: OptimizedRegularExpression::analyze built a bogus required
-- substring (e.g. "41" out of `\x41`) and the strstr pre-filter dropped matching rows
-- before re2 ran. `\x41` is the byte 'A', so these must behave like the literal 'A' forms.

SELECT match('Abc', '\\x41bc');               -- 1
SELECT match('Aaa', '\\101aa');               -- 1, octal \101 = 'A'
SELECT extract('Abc', '\\x41(bc)');           -- bc
SELECT extractAll('AbcAbc', '\\x41(bc)');     -- ['bc','bc']
SELECT countMatches('AbcAbc', '\\x41bc');     -- 2
SELECT match('Abc', '\\x{41}bc');             -- 1, \x{...} form

-- A `\x41`-prefixed regexp must match exactly the same rows as the literal 'A' form.
SELECT count()
FROM
(
    SELECT arrayJoin(['Abc', 'xAbcx', 'nope', 'ABC', 'abc']) AS s
    WHERE match(s, '\\x41bc') != match(s, 'Abc')
);                                            -- 0

-- Unicode property escapes (RE2): `\pL` is any letter, `\PN` is any non-number.
SELECT match('Xabc', '\\pLabc');              -- 1
SELECT match('Xabc', '\\PNabc');              -- 1
SELECT match('Xabc', '\\p{L}abc');            -- 1
SELECT match('Xabc', '\\P{N}abc');            -- 1

-- `\Q...\E` quoted literal: the body is literal text, including punctuation.
SELECT match('a(b)c', '\\Qa(b)c\\E');         -- 1
SELECT match('a(b)c', '\\Qa(b)c');            -- 1, unterminated \Q is literal to the end

-- Regression guards: short escapes and character classes must be unaffected.
SELECT match('A', '\\101');                   -- 1
SELECT match('5', '\\d');                     -- 1
SELECT match('x5abc', '\\dabc');              -- 1
