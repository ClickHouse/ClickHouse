-- `hasAny` with a constant array of strings must give the same answer as with the same array passed
-- as a non-constant argument, for `String`, `LowCardinality(String)` and `FixedString` elements, for blocks of one
-- and of 100 rows, and for long strings: needles of up to 256 bytes, a needle longer than that, and elements longer
-- than every needle.
DROP TABLE IF EXISTS t_has_any_const_strings;
-- Wide parts, so that `max_block_size` below splits the table into blocks of that size.
CREATE TABLE t_has_any_const_strings (id UInt64, s Array(String), lc Array(LowCardinality(String)), fs Array(FixedString(20)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_has_any_const_strings SELECT number, a, a, arrayMap(x -> toFixedString(left(x, 20), 20), a)
FROM (SELECT number, arrayMap(i -> [
        '', 'a', 'b', 'a\0', '\0a', 'tag1', 'tag12',
        concat(repeat('x', 8), 'A', repeat('y', 8)), concat(repeat('x', 8), 'B', repeat('y', 8)),
        repeat('z', 16), repeat('z', 17), repeat('q', 65), 'qq',
        concat('https://example.com/p/', toString(number % 40), '/index.html'),
        concat(repeat('L', 40), 'x', repeat('R', 40)), concat(repeat('L', 40), 'y', repeat('R', 40))][1 + cityHash64(number, i) % 16],
    range(number % 7)) AS a FROM numbers(3000));

WITH ['a'] AS ns SELECT '1 needle', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH ['a', 'qq'] AS ns SELECT '2 needles', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH ['', 'b', 'tag1'] AS ns SELECT '3 needles', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH [concat(repeat('x', 8), 'C', repeat('y', 8)), 'a\0', repeat('q', 65), 'absent'] AS ns SELECT '4 needles, same length and ends as elements', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH [concat(repeat('x', 8), 'A', repeat('y', 8)), 'tag12', repeat('z', 16), '', 'absent'] AS ns SELECT '5 needles', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH ['b', 'b', 'tag1', 'tag1', repeat('z', 17), '\0a', 'absent1', 'absent2'] AS ns SELECT '8 needles with duplicates', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH [concat(repeat('L', 40), 'x', repeat('R', 40)), concat(repeat('L', 40), 'z', repeat('R', 40)), repeat('q', 65), 'b', 'tag1'] AS ns SELECT 'long strings', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH (SELECT groupArray(concat('https://example.com/p/', toString(number * 2), '/index.html')) FROM numbers(33)) AS ns SELECT '33 needles', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))), countIf(hasAny(lc, ns) != hasAny(lc, materialize(ns))), countIf(hasAny(fs, ns) != hasAny(fs, materialize(ns))) FROM t_has_any_const_strings;
WITH ['absent1', 'absent2', 'absent3', 'absent4'] AS ns SELECT 'no needle present', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_strings;
WITH [] AS ns SELECT 'empty needle array', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_strings;
WITH [concat(repeat('x', 8), 'A', repeat('y', 8)), 'tag12', repeat('z', 16), '', 'absent'] AS ns SELECT 'one row per block', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_strings SETTINGS max_block_size = 1;
WITH [concat(repeat('x', 8), 'A', repeat('y', 8)), 'tag12', repeat('z', 16), '', 'absent'] AS ns SELECT '100-row blocks', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_strings SETTINGS max_block_size = 100;
WITH [NULL, 'a', 'b', 'tag1'] AS ns SELECT 'NULL needle', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_strings;
WITH ['a', 'b', 'tag1', 'qq'] AS ns SELECT 'Nullable elements', countIf(hasAny(CAST(s, 'Array(Nullable(String))'), ns)), countIf(hasAny(CAST(s, 'Array(Nullable(String))'), ns) != hasAny(CAST(s, 'Array(Nullable(String))'), materialize(ns))) FROM t_has_any_const_strings;
DROP TABLE t_has_any_const_strings;

DROP TABLE IF EXISTS t_has_any_const_long_strings;
CREATE TABLE t_has_any_const_long_strings (id UInt64, s Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_has_any_const_long_strings SELECT number, arrayMap(i -> [
        repeat('a', 256), repeat('a', 257),
        concat('head0000', repeat('m', 240), 'tail0000'), concat('head0000', repeat('m', 241), 'tail0000'),
        concat('head0000', repeat('x', 1008), 'tail0000'), concat('head0000', repeat('y', 1008), 'tail0000'),
        concat('head0000', repeat('x', 1007), 'z', 'tail0000'),
        repeat('b', 320), repeat('c', 321), 'a', '', repeat('q', 65)][1 + cityHash64(number, i) % 12],
    range(number % 5)) FROM numbers(2000);

WITH [repeat('a', 256), concat('head0000', repeat('n', 240), 'tail0000'), 'a', repeat('q', 65)] AS ns SELECT 'needles of up to 256 bytes', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_long_strings;
WITH [repeat('a', 257), concat('head0000', repeat('x', 1008), 'tail0000'), 'a', repeat('q', 65)] AS ns SELECT 'a needle longer than 256 bytes', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_long_strings;
WITH ['', 'a', repeat('q', 64), repeat('q', 65)] AS ns SELECT 'long elements, short needles', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_long_strings;
WITH [repeat('a', 256), 'a', repeat('q', 65), repeat('a', 257)] AS ns SELECT 'a needle longer than 256 bytes, last', countIf(hasAny(s, ns)), countIf(hasAny(s, ns) != hasAny(s, materialize(ns))) FROM t_has_any_const_long_strings;
DROP TABLE t_has_any_const_long_strings;
