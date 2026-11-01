-- Tests for JIT compilation of a subset of simple regular expressions in `match` and `extract`.
-- The JIT path must produce results identical to the general RE2 engine; patterns outside the
-- supported subset transparently fall back to RE2. `min_count_to_compile_regular_expression = 0`
-- forces compilation on first use so the JIT path is always exercised.

SET compile_regular_expressions = 1;
SET min_count_to_compile_regular_expression = 0;

SELECT '-- match: URL pattern from the design example';
SELECT match('http://example.com/path', '^https?://(?:www\\.)?([^/]+)/.*$');
SELECT match('https://www.example.com/', '^https?://(?:www\\.)?([^/]+)/.*$');
SELECT match('https://www.example.com', '^https?://(?:www\\.)?([^/]+)/.*$');
SELECT match('ftp://example.com/path', '^https?://(?:www\\.)?([^/]+)/.*$');
SELECT match('', '^https?://(?:www\\.)?([^/]+)/.*$');

SELECT '-- extract: capture groups';
SELECT extract('http://www.example.com/path', '^https?://(?:www\\.)?([^/]+)/.*$');
SELECT extract('https://example.com/a/b', '^https?://(?:www\\.)?([^/]+)/.*$');
SELECT extract('test@clickhouse.com', '.*@(.*)$');
SELECT extract('a@b@c', '.*@(.*)$');
SELECT extract('no_at_sign', '.*@(.*)$');
SELECT extract('2024-01-02', '^([0-9]{4})-');
SELECT extract('foo=bar', '^(\\w+)=');

SELECT '-- extract: whole match when there is no capturing group';
SELECT extract('hello123world', '[0-9]+');
SELECT extract('abc', '[0-9]+');

SELECT '-- anchors and quantifiers';
SELECT match('abc123', '^[a-z]+[0-9]+$');
SELECT match('abc123x', '^[a-z]+[0-9]+$');
SELECT match('12-3456', '^[0-9]{2}-[0-9]{4}$');
SELECT match('1-3456', '^[0-9]{2}-[0-9]{4}$');
SELECT match('aaa', '^a*$');
SELECT match('', '^a*$');
SELECT match('b', '^a+$');

SELECT '-- dot, dot-star and char classes';
SELECT match('anything at all', '.*');
SELECT match('x', '^.$');
SELECT match('foobar', '^foo[^0-9]*$');
SELECT match('foo123', '^foo[^0-9]*$');

SELECT '-- case-insensitive (ASCII)';
SELECT match('HELLO', '(?i)^hello$');
SELECT match('HeLLo', '(?i)^hello$');
SELECT match('hella', '(?i)^hello$');
SELECT extract('USER42', '(?i)^([a-z]+)');

SELECT '-- case-insensitive folding of k/s reaches non-ASCII code points in RE2 (U+212A KELVIN SIGN, U+017F LONG S); JIT must agree (it falls back)';
SELECT
    (SELECT match(char(0xE2, 0x84, 0xAA), '(?i)^k$') SETTINGS compile_regular_expressions = 1)
  = (SELECT match(char(0xE2, 0x84, 0xAA), '(?i)^k$') SETTINGS compile_regular_expressions = 0);
SELECT
    (SELECT match(char(0xC5, 0xBF), '(?i)^[s]$') SETTINGS compile_regular_expressions = 1)
  = (SELECT match(char(0xC5, 0xBF), '(?i)^[s]$') SETTINGS compile_regular_expressions = 0);
SELECT
    (SELECT extract(concat('a', char(0xE2, 0x84, 0xAA), 'b'), '(?i)^([a-z]+)') SETTINGS compile_regular_expressions = 1)
  = (SELECT extract(concat('a', char(0xE2, 0x84, 0xAA), 'b'), '(?i)^([a-z]+)') SETTINGS compile_regular_expressions = 0);

SELECT '-- case-insensitive negated classes must fall back too: a byte-wise scan would let `[^a-z]` accept the bytes of KELVIN SIGN / LONG S, while RE2 folds them to k/s and rejects them';
SELECT
    (SELECT match(char(0xE2, 0x84, 0xAA), '(?i)^[^a-z]+$') SETTINGS compile_regular_expressions = 1)
  = (SELECT match(char(0xE2, 0x84, 0xAA), '(?i)^[^a-z]+$') SETTINGS compile_regular_expressions = 0);
SELECT
    (SELECT match(char(0xC5, 0xBF), '(?i)^[^a-z]+$') SETTINGS compile_regular_expressions = 1)
  = (SELECT match(char(0xC5, 0xBF), '(?i)^[^a-z]+$') SETTINGS compile_regular_expressions = 0);
-- A negated class that does not depend on k/s stays compiled and keeps matching high bytes, exactly like RE2.
SELECT
    (SELECT match(char(0xE2, 0x84, 0xAA), '(?i)^[^/]+$') SETTINGS compile_regular_expressions = 1)
  = (SELECT match(char(0xE2, 0x84, 0xAA), '(?i)^[^/]+$') SETTINGS compile_regular_expressions = 0);

SELECT '-- oversized patterns fall back to RE2 before JIT code generation (bounded LLVM compilation)';
-- A `^` + large literal + `$` is in the supported subset but must not be compiled (one comparison per
-- byte would make LLVM build an enormous function); it falls back to RE2 and stays correct.
SELECT match(repeat('a', 2000), concat('^', repeat('a', 2000), '$'));
SELECT match(concat(repeat('a', 1999), 'b'), concat('^', repeat('a', 2000), '$'));
SELECT
    (SELECT match(repeat('a', 2000), concat('^', repeat('a', 2000), '$')) SETTINGS compile_regular_expressions = 1)
  = (SELECT match(repeat('a', 2000), concat('^', repeat('a', 2000), '$')) SETTINGS compile_regular_expressions = 0);

SELECT '-- empty string and empty match edge cases';
SELECT match('', '^$');
SELECT match('x', '^$');
SELECT extract('', '^(.*)$');

SELECT '-- patterns outside the subset fall back to RE2 (results must still be correct)';
SELECT match('cat', 'cat|dog');
SELECT match('dog', 'cat|dog');
SELECT match('the cat sat', '\\bcat\\b');
SELECT extract('id=123;', 'id=(\\d+?);');

SELECT '-- vectorized over many rows, exercising the SIMD scan and the tail';
SELECT sum(match(concat('id', toString(number), '/x'), '^id[0-9]+/.*$')) FROM numbers(1000);
SELECT count() FROM numbers(1000) WHERE match(concat('id', toString(number)), '^id[0-9]+/.*$');
SELECT sum(length(extract(concat('user', toString(number), '@host.com'), '^(\\w+)@'))) FROM numbers(100);
SELECT match(concat(repeat('a', 100), '/'), '^a+/$');
SELECT match(repeat('a', 100), '^a+/$');
SELECT extract(concat(repeat('x', 50), '@', repeat('y', 50)), '.*@(.*)$') = repeat('y', 50);

SELECT '-- the JIT result matches the RE2 result for the same patterns';
SELECT
    sum(match(s, '^https?://(?:www\\.)?([^/]+)/.*$')) AS jit
FROM (SELECT arrayJoin(['http://a.com/x', 'https://www.b.org/', 'no', 'https://c/']) AS s)
SETTINGS compile_regular_expressions = 1, min_count_to_compile_regular_expression = 0;
SELECT
    sum(match(s, '^https?://(?:www\\.)?([^/]+)/.*$')) AS re2
FROM (SELECT arrayJoin(['http://a.com/x', 'https://www.b.org/', 'no', 'https://c/']) AS s)
SETTINGS compile_regular_expressions = 0;
