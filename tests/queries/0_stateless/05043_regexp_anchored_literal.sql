-- Anchored literal patterns (`^lit`, `lit$`, `^lit$`) are matched by a comparison, without running `re2`.

SELECT 'Prefix, suffix and exact literals, down to a single character';

SELECT extract('abcdef', '^abc');
SELECT extract('xabcdef', '^abc');
SELECT extract('ab', '^abc');
SELECT extract('', '^abc');
SELECT extract('abcdef', 'def$');
SELECT extract('abcdefx', 'def$');
SELECT extract('ef', 'def$');
SELECT extract('', 'def$');
SELECT extract('abc', '^abc$');
SELECT extract('abcd', '^abc$');
SELECT extract('xabc', '^abc$');
SELECT extract('', '^abc$');
SELECT extract('abc', '^a');
SELECT extract('xabc', '^a');
SELECT extract('abc', 'c$');
SELECT extract('abcx', 'c$');

SELECT 'End of text (`$` is not the end of a line), escapes and multi-byte characters';

SELECT extract('abc\n', 'abc$');
SELECT extract('abc\n', '^abc$');
SELECT extract('\nabc', '^abc');
SELECT extract('abc\n', '^abc');
SELECT extract('a$b', 'a\\$b$');
SELECT extract('^abc', '\\^abc');
SELECT extract('a.c', '^a\\.c$');
SELECT extract('abc', '^a\\.c$');
SELECT extract('абвгд', '^абв');
SELECT extract('абвгд', 'вгд$');
SELECT extract('xабв', '^абв');
SELECT extract('абв', '^абв$');

SELECT 'Patterns that stay on the re2 path';

SELECT extract('abc', '^');
SELECT extract('abc', '$');
SELECT extract('', '^$');
SELECT extract('a', '^$');
SELECT extract('xabcx', 'abc');
SELECT extract('xabcx', 'a.c');
SELECT extract('xabcx', '^a|c$');

SELECT 'Match offsets';

SELECT countMatches('abcabc', '^abc');
SELECT countMatches('abcabc', 'abc$');
SELECT countMatches('aaa', '^a');
SELECT countMatches('aaa', 'a$');
SELECT regexpPosition('abcabc', '^abc');
SELECT regexpPosition('xabc', '^abc');
SELECT regexpPosition('abcabc', 'abc$');
SELECT regexpPosition('abcx', 'abc$');
SELECT splitByRegexp('^a', 'aXaY');
SELECT splitByRegexp('a$', 'XaYa');
SELECT splitByRegexp('^ab$', 'ab');
SELECT splitByRegexp('b$', 'abab');

SELECT 'Vectorized match and like';

SELECT match(s, '^abc') FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s) ORDER BY s;
SELECT match(s, 'abc$') FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s) ORDER BY s;
SELECT match(s, '^abc$') FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s) ORDER BY s;
SELECT s LIKE 'abc%' FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s) ORDER BY s;
SELECT s LIKE '%abc' FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s) ORDER BY s;
SELECT s LIKE 'abc' FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s) ORDER BY s;

SELECT match(f, '^abc') FROM (SELECT toFixedString(s, 8) AS f FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s)) ORDER BY f;
SELECT match(f, 'abc$') FROM (SELECT toFixedString(s, 8) AS f FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s)) ORDER BY f;
SELECT match(f, '^abc$') FROM (SELECT toFixedString(s, 8) AS f FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s)) ORDER BY f;

SELECT 'non-constant pattern, matched row by row';

SELECT s, p, match(s, p) FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s, arrayJoin(['^abc', 'abc$', '^abc$']) AS p) ORDER BY p, s;
SELECT f, p, match(f, p) FROM (SELECT toFixedString(s, 8) AS f, p FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s, arrayJoin(['^abc', 'abc$', '^abc$']) AS p)) ORDER BY p, f;
SELECT p, match('abcabc', p) FROM (SELECT arrayJoin(['^abc', 'abc$', '^abc$', '^abcabc$']) AS p) ORDER BY p;
SELECT s, p, s LIKE p FROM (SELECT arrayJoin(['abc', 'xabc', 'abcx', 'ab', '', 'abcabc', 'xabcabc', 'abcabcx']) AS s, arrayJoin(['abc%', '%abc', 'abc']) AS p) ORDER BY p, s;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x UInt8) ENGINE = Memory;
CREATE TABLE t2 (x UInt8) ENGINE = Memory;

INSERT INTO t1 VALUES (1);
INSERT INTO t2 VALUES (2);

SELECT 'The merge table function matches table names with a regexp: exact, prefix, suffix';

SELECT sum(x) FROM merge(currentDatabase(), '^t1$');
SELECT sum(x) FROM merge(currentDatabase(), '^t1');
SELECT sum(x) FROM merge(currentDatabase(), '2$');

DROP TABLE t1;
DROP TABLE t2;

SELECT 'An escape of a non-alphanumeric character is part of the literal, so it stays on the fast path';

-- `RE2::QuoteMeta` escapes every non-alphanumeric byte, so its output arrives as `\%`, `\ `, `\:` and so on.
SELECT extract('100%-alpha', '^100\\%');
SELECT extract('x100%-alpha', '^100\\%');
SELECT extract('a b', '^a\\ b$');
SELECT extract('a:b', '^a\\:b$');
SELECT extract('svc,4999', 'svc\\,4999$');
SELECT extract('user@host', '^user\\@host$');
SELECT extract('a#b', '^a\\#b');
SELECT extract('a=b', '^a\\=b');
SELECT extract('a~b', '^a\\~b');
SELECT extract('a_b', '^a\\_b');
SELECT extract('a\\b', '^a\\\\b');
SELECT extract('xa b', '^a\\ b');
SELECT extract('a bx', '^a\\ b');

SELECT 'An escape of an alphanumeric character is a special sequence and stays on the re2 path';

SELECT extract('a1', '^a\\d');
SELECT extract('aXb', '^a\\wb');
SELECT extract('A', '^\\x41$');
SELECT extract('ab', '^a\\x62$');
SELECT extract('a\tb', '^a\\tb$');

SELECT 'A column haystack with an escaped literal, where the first row matches';

SELECT match(s, '^a\\ b') FROM (SELECT arrayJoin(['a b', 'xa b', 'a bx', 'ab', '']) AS s) ORDER BY s;
SELECT match(s, 'a\\ b$') FROM (SELECT arrayJoin(['a b', 'xa b', 'a bx', 'ab', '']) AS s) ORDER BY s;
SELECT match(s, '^a\\ b$') FROM (SELECT arrayJoin(['a b', 'xa b', 'a bx', 'ab', '']) AS s) ORDER BY s;
SELECT match(s, '^a\\%b') FROM (SELECT arrayJoin(['a%b', 'xa%b', 'a%bx', 'ab', '']) AS s) ORDER BY s;
SELECT match(s, '^a\\:b$') FROM (SELECT arrayJoin(['a:b', 'xa:b', 'a:bx', 'ab', '']) AS s) ORDER BY s;
SELECT match(f, '^a\\ b') FROM (SELECT toFixedString(s, 5) AS f FROM (SELECT arrayJoin(['a b', 'xa b', 'a bx']) AS s)) ORDER BY f;
