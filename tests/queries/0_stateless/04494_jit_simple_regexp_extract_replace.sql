-- Tests for JIT compilation of simple regular expressions in `extractAll`, `replaceRegexpOne` and
-- `replaceRegexpAll`. The JIT path must produce results identical to the general RE2 engine.
-- `min_count_to_compile_regular_expression = 0` forces compilation on first use.

SET compile_regular_expressions = 1;
SET min_count_to_compile_regular_expression = 0;

SELECT '-- extractAll';
SELECT extractAll('hello 123 world 456', '[0-9]+');
SELECT extractAll('a1b22c333', '[0-9]+');
SELECT extractAll('test@example.com, user@domain.org', '([a-zA-Z0-9]+)@');
SELECT extractAll('no digits here', '[0-9]+');
SELECT extractAll('', '[0-9]+');
SELECT extractAll('/a/bb/ccc/', '[^/]+');
SELECT extractAll('key1=val1&key2=val2', '([a-z0-9]+)=([a-z0-9]+)');

SELECT '-- replaceRegexpOne';
SELECT replaceRegexpOne('hello 123 world 456', '[0-9]+', 'N');
SELECT replaceRegexpOne('2024-01-02', '([0-9]+)-([0-9]+)', '\\2/\\1');
SELECT replaceRegexpOne('abc', 'x+', 'Z');
SELECT replaceRegexpOne('user@host.com', '^(\\w+)@(\\w+)', '\\2.\\1');

SELECT '-- replaceRegexpAll';
SELECT replaceRegexpAll('hello 123 world 456', '[0-9]+', 'N');
SELECT replaceRegexpAll('a.b.c', '\\.', '/');
SELECT replaceRegexpAll('path/to/file', '/', '_');
SELECT replaceRegexpAll('aaa', 'a', 'bb');
SELECT replaceRegexpAll('one two three', '(\\w+)', '[\\1]');
SELECT replaceRegexpAll('https://www.example.com/p', '^https?://(?:www\\.)?', '');
SELECT replaceRegexpAll('x', '', '-');
SELECT replaceRegexpAll('abc', '^|$', '|');

SELECT '-- dot vs newline: extractAll and replaceRegexp* both build RE2 with RE_DOT_NL / dot_nl, so . matches newline';
SELECT replaceRegexpAll('a\nb', '.', 'X');
SELECT extractAll('a\nb', '.');
SELECT replaceRegexpAll('a\nb\nc', '.', 'X');
-- a quantified dot must also span newlines (the JIT matcher must agree with RE2's dot_nl mode)
SELECT replaceRegexpOne('a\nb', '.+', 'X');
SELECT replaceRegexpAll('a\nb\nc', '.+', 'X');

SELECT '-- vectorized over many rows, JIT path equals RE2 (one side compiled, the other forced onto RE2)';
SELECT
    (SELECT sum(cityHash64(replaceRegexpAll(concat('id', toString(number), '/x', toString(number)), '[0-9]+', '#'))) FROM numbers(1000) SETTINGS compile_regular_expressions = 1)
  = (SELECT sum(cityHash64(replaceRegexpAll(concat('id', toString(number), '/x', toString(number)), '[0-9]+', '#'))) FROM numbers(1000) SETTINGS compile_regular_expressions = 0);
SELECT
    (SELECT sum(length(extractAll(concat('a', toString(number), 'b', toString(number % 7)), '[0-9]+'))) FROM numbers(1000) SETTINGS compile_regular_expressions = 1)
  = (SELECT sum(length(extractAll(concat('a', toString(number), 'b', toString(number % 7)), '[0-9]+'))) FROM numbers(1000) SETTINGS compile_regular_expressions = 0);

SELECT '-- patterns outside the subset fall back to RE2';
SELECT extractAll('a1b2', '\\d+?');
SELECT replaceRegexpAll('cat dog cat', 'cat|dog', 'X');

SELECT '-- ReDoS-shaped patterns must fall back (no exponential backtracking) and stay correct/fast';
-- If the no-backtracking gate were broken, JIT-compiling these and matching the long inputs below
-- would blow up exponentially and time out; they must fall back to RE2 and return instantly.
SELECT match(repeat('a', 64), 'a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?b');
SELECT match(concat(repeat('a', 64), 'b'), 'a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?a?b');
SELECT match(repeat('x', 100000), '.*.*.*.*y');
SELECT match(repeat('x', 100000), '^(a*)*b$');
SELECT replaceRegexpAll(repeat('a', 1000), 'a?a?a?a?a?a?a?a?b', 'Z') = repeat('a', 1000);

SELECT '-- replaceRegexpOne with a single-group replacement';
SELECT replaceRegexpOne(materialize('https://www.example.com/path?q=1'), '^https?://(?:www\\.)?([^/]+)/.*$', '\\1');
SELECT replaceRegexpOne(materialize('ftp://example.com/x'), '^https?://(?:www\\.)?([^/]+)/.*$', '\\1');
SELECT replaceRegexpOne(materialize(''), '^https?://(?:www\\.)?([^/]+)/.*$', '\\1');
SELECT replaceRegexpOne(materialize('xx-ab-yy-cd'), '-([a-z]+)-', '\\1');
SELECT replaceRegexpOne(materialize('abc123def'), '[0-9]+', '\\0');
SELECT replaceRegexpOne(materialize('abc'), '(x)?b', '\\1');
SELECT hex(replaceRegexpOne(materialize(concat('a\n', char(0), 'b\n', char(0), 'c')), 'b(.+)', '\\1'));
SELECT
    (SELECT sum(cityHash64(number, replaceRegexpOne(s, '^([a-z]+)/(.*)$', '\\1'))) FROM (SELECT number, repeat(concat(if(number % 5 = 0, 'X', char(97 + number % 26)), '/'), number % 40) AS s FROM numbers(3000)) SETTINGS compile_regular_expressions = 1, max_block_size = 7)
  = (SELECT sum(cityHash64(number, replaceRegexpOne(s, '^([a-z]+)/(.*)$', '\\1'))) FROM (SELECT number, repeat(concat(if(number % 5 = 0, 'X', char(97 + number % 26)), '/'), number % 40) AS s FROM numbers(3000)) SETTINGS compile_regular_expressions = 0, max_block_size = 7);
SELECT
    (SELECT sum(cityHash64(number, replaceRegexpOne(s, '^([a-z]+)/(.*)$', '\\2'))) FROM (SELECT number, repeat(concat(if(number % 5 = 0, 'X', char(97 + number % 26)), '/'), number % 40) AS s FROM numbers(3000)) SETTINGS compile_regular_expressions = 1)
  = (SELECT sum(cityHash64(number, replaceRegexpOne(s, '^([a-z]+)/(.*)$', '\\2'))) FROM (SELECT number, repeat(concat(if(number % 5 = 0, 'X', char(97 + number % 26)), '/'), number % 40) AS s FROM numbers(3000)) SETTINGS compile_regular_expressions = 0);
SELECT
    (SELECT sum(cityHash64(number, replaceRegexpOne(s, '(?:x)?([0-9]+)', '\\1'))) FROM (SELECT number, multiIf(number % 11 = 0, '', number % 7 = 0, 'no digits in this row at all', concat('id', if(number % 2 = 0, 'x', ''), toString(number), '/', repeat('y', number % 30))) AS s FROM numbers(3000)) SETTINGS compile_regular_expressions = 1)
  = (SELECT sum(cityHash64(number, replaceRegexpOne(s, '(?:x)?([0-9]+)', '\\1'))) FROM (SELECT number, multiIf(number % 11 = 0, '', number % 7 = 0, 'no digits in this row at all', concat('id', if(number % 2 = 0, 'x', ''), toString(number), '/', repeat('y', number % 30))) AS s FROM numbers(3000)) SETTINGS compile_regular_expressions = 0);
