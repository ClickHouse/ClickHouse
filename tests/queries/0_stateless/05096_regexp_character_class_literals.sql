-- RE2 reads a `]` written first in a character class, and a `[` inside an open one, as literal
-- members of that class. The analyzer that extracts a required substring for the `match` prefilter
-- did not: it closed the class at the first `]` and opened another class at every `[`, so it required
-- a substring the pattern does not - and `match` answered 0 for rows RE2 matches.

SELECT 'a class whose first member is a literal ]';
SELECT match(materialize('ab'), '[]a]b'), countMatches(materialize('ab'), '[]a]b');
SELECT match(materialize(']b'), '[]a]b'), match(materialize('zz'), '[]a]b');
SELECT match(materialize('xb'), '[^]a]b'), match(materialize('ab'), '[^]a]b');
SELECT replaceRegexpOne(materialize('ab'), '[]a]b', 'X'), extract(materialize('ab'), '[]a]b');

SELECT 'a class containing a literal [ next to an alternation';
SELECT match(materialize('zb'), 'abc[[]|b'), match(materialize('x'), 'abc[[]|');
-- `countMatches` counts nothing for a pattern whose only match is empty, as it does for `'a|'`, so
-- count the non-empty match of the left alternative instead.
SELECT countMatches(materialize('zb'), 'abc[[]|b'), match(materialize('abc['), 'abc[[]|b');
SELECT replaceRegexpOne(materialize('zb'), 'abc[[]|b', 'X'), replaceRegexpOne(materialize('x'), 'abc[[]|', 'X');

SELECT 'the same over a table, where the prefilter is used';
DROP TABLE IF EXISTS t_regexp_class;
CREATE TABLE t_regexp_class (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_regexp_class VALUES (1, 'ab'), (2, ']b'), (3, 'zz'), (4, 'x'), (5, 'abc[');

SELECT count() FROM t_regexp_class WHERE match(s, '[]a]b');
SELECT countIf(match(s, '[]a]b')) FROM t_regexp_class;
SELECT count() FROM t_regexp_class WHERE match(s, 'abc[[]|');
SELECT countIf(match(s, 'abc[[]|')) FROM t_regexp_class;

SELECT 'a pattern with a non-constant needle takes the same path';
SELECT count() FROM t_regexp_class WHERE match(s, concat('[]a', materialize(']b')));

SELECT 'ordinary classes still prefilter the same way';
SELECT count() FROM t_regexp_class WHERE match(s, '[az]b');
SELECT countIf(match(s, '[az]b')) FROM t_regexp_class;
SELECT count() FROM t_regexp_class WHERE match(s, 'a[b]');
SELECT count() FROM t_regexp_class WHERE match(s, '^zz$');

DROP TABLE t_regexp_class;
