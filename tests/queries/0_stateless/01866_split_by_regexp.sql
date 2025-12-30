select splitByRegexp('\\d+', x) from (select arrayJoin(['a1ba5ba8b', 'a11ba5689ba891011b']) x);
select splitByRegexp('', 'abcde');
select splitByRegexp('<[^<>]*>', x) from (select arrayJoin(['<h1>hello<h2>world</h2></h1>', 'gbye<split>bug']) x);
select splitByRegexp('ab', '');
select splitByRegexp('', '');

SELECT 'Test fallback of splitByRegexp to splitByChar if regexp is trivial';
select splitByRegexp(' ', 'a b c');
select splitByRegexp('-', 'a-b-c');
select splitByRegexp('.', 'a.b.c');
select splitByRegexp('^', 'a^b^c');
select splitByRegexp('$', 'a$b$c');
select splitByRegexp('+', 'a+b+c'); -- { serverError CANNOT_COMPILE_REGEXP }
select splitByRegexp('?', 'a?b?c'); -- { serverError CANNOT_COMPILE_REGEXP }
select splitByRegexp('(', 'a(b(c'); -- { serverError CANNOT_COMPILE_REGEXP }
select splitByRegexp(')', 'a)b)c');
select splitByRegexp('[', 'a[b[c'); -- { serverError CANNOT_COMPILE_REGEXP }
select splitByRegexp(']', 'a]b]c');
select splitByRegexp('{', 'a{b{c');
select splitByRegexp('}', 'a}b}c');
select splitByRegexp('|', 'a|b|c');
select splitByRegexp('\\', 'a\\b\\c');

SELECT 'AST Fuzzer failure';
SELECT splitByRegexp(materialize(1), NULL, 3) -- { serverError ILLEGAL_COLUMN }
