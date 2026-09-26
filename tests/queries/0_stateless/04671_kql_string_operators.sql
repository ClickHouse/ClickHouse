-- KQL string operators.
--
-- These used to be translated by pasting the needle between two `%` signs and handing the
-- result to `ilike`. That made every LIKE metacharacter in a needle significant, and let a
-- needle containing a quote escape into the generated SQL. Both are covered below.
--
-- Each case prints the predicate itself, so the reference records the answer rather than
-- the presence of a row.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- contains is a substring test, not a LIKE pattern --';
print p = '50x' contains '50%';         // false: '%' is a literal per cent sign
print p = '50%off' contains '50%';      // true
print p = 'aXc' contains 'a_c';         // false: '_' is a literal underscore
print p = 'a_c' contains 'a_c';         // true
print p = 'ABC' contains 'abc';         // true: contains ignores case
print p = 'ABC' contains_cs 'abc';      // false: contains_cs does not
print p = 'a.c' contains '.';           // true
print p = 'abc' contains '.';           // false: '.' is not a regex wildcard here

print '-- a needle cannot inject SQL --';
-- Before the rewrite the first of these returned a row: the needle closed the generated
-- string literal and everything after it was parsed as part of the surrounding expression.
print p = 'hello' contains "zzzz') OR 1 = 1 OR ilike(s, 'q";
print p = 'hello' contains "zzzz') OR (SELECT count() FROM numbers(10)) > 0 OR ilike(s, 'q";
print p = 'hello' contains "'; DROP TABLE x; --";

print '-- startswith / endswith --';
print p = 'abcdef' startswith 'ABC';
print p = 'abcdef' startswith_cs 'ABC';
print p = 'abcdef' endswith 'DEF';
print p = 'abcdef' endswith_cs 'DEF';
print p = 'abcdef' startswith '%';

print '-- has matches whole terms --';
print p = 'a-b' has 'a';                // true: '-' separates terms
print p = 'abc' has 'ab';               // false: 'ab' is not a whole term
print p = 'a.b' has 'b';                // true
print p = 'a_b' has 'a';                // true: '_' is a separator in Kusto
print p = 'ABC def' has 'abc';          // true
print p = 'ABC def' has_cs 'abc';       // false
print p = 'hello world' hasprefix 'wor';
print p = 'hello world' hasprefix 'orl';
print p = 'hello world' hassuffix 'rld';
print p = 'hello world' hassuffix 'wor';

print '-- negated forms --';
print p = 'abc' !contains 'zzz';
print p = 'abc' !has 'zzz';
print p = 'abc' !startswith 'zzz';

print '-- has_any / has_all --';
print p = 'alpha beta' has_any ('zzz', 'beta');
print p = 'alpha beta' has_all ('alpha', 'beta');
print p = 'alpha beta' has_all ('alpha', 'zzz');

print '-- in --';
print p = 'b' in ('a', 'b');
print p = 'B' in ('a', 'b');            // false: in is case-sensitive
print p = 'B' in~ ('a', 'b');           // true
print p = 'c' !in ('a', 'b');

print '-- between --';
print p = 5 between (1 .. 10);
print p = 50 between (1 .. 10);
print p = 50 !between (1 .. 10);

print '-- =~ and !~ --';
print p = 'ABC' =~ 'abc';
print p = 'ABC' != 'abc';
print p = 'ABC' !~ 'abd';

print '-- matches regex --';
print p = 'abc123' matches regex '[a-c]+[0-9]+';
print p = 'zzz' matches regex '[a-c]+[0-9]+';

print '-- the operators still filter --';
datatable (S:string) ['alpha', 'beta', 'gamma'] | where S contains 'a' | project S;
datatable (S:string) ['a-b', 'abc'] | where S has 'a' | project S;

print '-- verbatim strings keep backslashes --';
print @'a\b';
print 'a\\b';
print strlen(@'a\b');

SET dialect = 'clickhouse';
