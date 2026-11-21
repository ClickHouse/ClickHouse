-- A constant pattern may cache results per distinct haystack, depending on measured timing; a per-row pattern
-- never does, so it is the reference. The JIT implementation does not cache, so it is kept out of the way.
SET min_count_to_compile_regular_expression = 1000000;

-- Non-adjacent repeats.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(number % 7), 'cd', repeat('z', number % 5)) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- Adjacent repeats.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(intDiv(number, 4) % 3), 'cd', repeat('z', intDiv(number, 4) % 5)) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- Only distinct values.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(number), 'cd') AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- Repeats arriving after a stretch of distinct values.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(number < 1000, concat('ab', toString(number), 'cd'), concat('ab', toString(intDiv(number, 8) % 4), 'cd')) AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- A mostly-distinct prefix followed by a short non-adjacent cycle.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(number < 256, concat('ab', toString(number), 'cd'), concat('ab', toString(number % 4), 'cd')) AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- The same with a nine-value cycle, whose length shares no factor with the 32-row window.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(number < 256, concat('ab', toString(number), 'cd'), concat('ab', toString(number % 9), 'cd')) AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- A distinct prefix followed by one repeated value in every block.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(number % 500 < 256, concat('ab', toString(number), 'cd'), 'ab7cd') AS h FROM numbers(2000))
SETTINGS max_block_size = 500;

-- Alternating mostly-distinct and low-cardinality stretches within one block.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(intDiv(number, 512) % 2 = 0, concat('ab', toString(number), 'cd'), concat('ab', toString(number % 4), 'cd')) AS h FROM numbers(4096))
SETTINGS max_block_size = 4096;

-- A repetitive prefix followed by a mostly-distinct suffix.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(number < 400, 'ab1cd', concat('ab', toString(number), 'cd')) AS h FROM numbers(4000))
SETTINGS max_block_size = 4000;

-- Repeats within many small blocks, where the cache is built and dropped again for every block. The cycle is
-- shorter than the block, or no value would recur before the block ends and nothing could be deduplicated.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(number % 3), 'cd', repeat('z', number % 3)) AS h FROM numbers(1000))
SETTINGS max_block_size = 5;

-- Cached ranges of length zero, next to repeats whose result is not empty.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), ''))
FROM (SELECT if(number % 3 = 0, '', concat('ab', toString(number % 7), '12', substring('QRSTU', 1, number % 5))) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- Results of differing length next to each other.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2\\2\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2\\2\\2:\\1'))
FROM (SELECT concat(repeat('a', number % 11 + 1), toString(number % 13)) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

SELECT countIf(replaceRegexpOne(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpOne(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(number % 7), 'cd', toString(number % 3)) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- A `FixedString` haystack has an entry point of its own. `rightPad` makes every value exactly as long as the
-- `FixedString`, so no padding is added and the `String` reference sees the same bytes.
SELECT countIf(replaceRegexpAll(toFixedString(h, 12), '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT rightPad(concat('ab', toString(number % 7), 'cd', repeat('z', number % 5)), 12, 'q') AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

SELECT countIf(replaceRegexpAll(toFixedString(h, 12), '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT rightPad(concat('ab', toString(number), 'cd'), 12, 'q') AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- No row starts with the literal after `^`, so every row is copied through unchanged ahead of the regexp.
SELECT countIf(replaceRegexpOne(h, '^missing', 'X') != replaceRegexpOne(h, materialize('^missing'), 'X'))
FROM (SELECT concat('ab', toString(number % 7), 'cd') AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- The same in short blocks.
SELECT countIf(replaceRegexpOne(h, '^missing', 'X') != replaceRegexpOne(h, materialize('^missing'), 'X'))
FROM (SELECT concat('ab', toString(number % 7), 'cd') AS h FROM numbers(1000))
SETTINGS max_block_size = 100;

-- Adjacent repeats without any match.
SELECT countIf(replaceRegexpAll(h, '^missing', 'X') != replaceRegexpAll(h, materialize('^missing'), 'X'))
FROM (SELECT concat('ab', toString(intDiv(number, 4) % 3), 'cd') AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- The pattern below is anchored and has a capturing group, but no literal right after `^`, so every row reaches
-- the regexp and the capture-free pre-check engages once a window is almost entirely non-matching.

-- Almost no row matches, so the pre-check engages, while the sparse matching rows can still be cached.
SELECT countIf(replaceRegexpAll(h, '^[ab]b([0-9]+)', '<\\1>') != replaceRegexpAll(h, materialize('^[ab]b([0-9]+)'), '<\\1>'))
FROM (SELECT if(number % 100 = 50, concat('ab', toString(number % 7), 'cd'), concat('xx', toString(number % 13), 'yy')) AS h FROM numbers(5000))
SETTINGS max_block_size = 5000;

-- Almost no row matches and every haystack is distinct, so the pre-check stays on and the sparse matching rows
-- pass it.
SELECT countIf(replaceRegexpAll(h, '^[ab]b([0-9]+)', '<\\1>') != replaceRegexpAll(h, materialize('^[ab]b([0-9]+)'), '<\\1>'))
FROM (SELECT if(number % 64 = 0, concat('ab', toString(number), 'cd'), concat('xx', toString(number), 'yy')) AS h FROM numbers(4000))
SETTINGS max_block_size = 4000;

-- A rejecting distinct prefix followed by a matching distinct suffix: the pre-check engages on the prefix, and
-- the re-evaluated match ratio turns it off again on the suffix.
SELECT countIf(replaceRegexpOne(h, '^[ab]b([0-9]+)', '<\\1>') != replaceRegexpOne(h, materialize('^[ab]b([0-9]+)'), '<\\1>'))
FROM (SELECT if(number < 1000, concat('xx', toString(number), 'yy'), concat('ab', toString(number), 'cd')) AS h FROM numbers(4000))
SETTINGS max_block_size = 4000;

-- Every row is the same haystack without the literal after `^`.
SELECT countIf(replaceRegexpOne(h, '^missing', 'X') != replaceRegexpOne(h, materialize('^missing'), 'X'))
FROM (SELECT materialize('abcdefgh') AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- A run of one repeated rejecting value after distinct rejecting values.
SELECT countIf(replaceRegexpOne(h, '^missing', 'X') != replaceRegexpOne(h, materialize('^missing'), 'X'))
FROM (SELECT if(number < 500, concat('ab', toString(number), 'cd'), 'repeatedvalue') AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- Non-adjacent repeats of rejecting values of an unanchored pattern, whose rejects scan the whole haystack
-- and can be cached like matches.
SELECT countIf(replaceRegexpOne(h, '[0-9]{50}', 'X') != replaceRegexpOne(h, materialize('[0-9]{50}'), 'X'))
FROM (SELECT concat('ab', toString(number % 7), 'cd', repeat('z', number % 5)) AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- Alternating rejecting and matching runs, each longer than the window: the windowed match ratio turns the
-- pre-check on and off repeatedly within one block.
SELECT countIf(replaceRegexpOne(h, '^[ab]b([0-9]+)', '<\\1>') != replaceRegexpOne(h, materialize('^[ab]b([0-9]+)'), '<\\1>'))
FROM (SELECT if(intDiv(number, 100) % 2 = 0, concat('xx', toString(number), 'yy'), concat('ab', toString(number), 'cd')) AS h FROM numbers(4000))
SETTINGS max_block_size = 4000;

-- The values a repeat is expected to produce, so that the cases above cannot pass by both sides being wrong.
SELECT DISTINCT replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1')
FROM (SELECT concat('ab', toString(number % 3), 'cd', toString(number % 2)) AS h FROM numbers(100))
ORDER BY 1;
