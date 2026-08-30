-- A constant pattern runs the regexp once per distinct haystack of a block and copies the cached result for the
-- repeats; a per-row pattern takes the plain path, which makes it the reference to compare against.
-- The JIT implementation processes every row directly, so it is kept out of the way.
SET min_count_to_compile_regular_expression = 1000000;

-- Non-adjacent repeats.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(number % 7), 'cd', repeat('z', number % 5)) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- Adjacent repeats.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(intDiv(number, 4) % 3), 'cd', repeat('z', intDiv(number, 4) % 5)) AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- Only distinct values, past the row count at which the map is switched off.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT concat('ab', toString(number), 'cd') AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- Repeats arriving once the map is off.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(number < 1000, concat('ab', toString(number), 'cd'), concat('ab', toString(intDiv(number, 8) % 4), 'cd')) AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- Repeats within many small blocks, where the cache is built and dropped again for every block. The cycle is
-- shorter than the block, or no value would recur before the block ends and nothing would be deduplicated.
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

-- No row matches at all: past the first ratio check the capture-free pre-check takes over and copies
-- every row through unchanged.
SELECT countIf(replaceRegexpOne(h, '^missing', 'X') != replaceRegexpOne(h, materialize('^missing'), 'X'))
FROM (SELECT concat('ab', toString(number % 7), 'cd') AS h FROM numbers(1000))
SETTINGS max_block_size = 1000;

-- No row matches and the blocks are shorter than the distinct-ratio sample: the early match-ratio
-- checkpoint must still engage the pre-check inside each block.
SELECT countIf(replaceRegexpOne(h, '^missing', 'X') != replaceRegexpOne(h, materialize('^missing'), 'X'))
FROM (SELECT concat('ab', toString(number % 7), 'cd') AS h FROM numbers(1000))
SETTINGS max_block_size = 100;

-- Adjacent repeats without any match: once the pre-check is on, it runs before the previous-value compare.
SELECT countIf(replaceRegexpAll(h, '^missing', 'X') != replaceRegexpAll(h, materialize('^missing'), 'X'))
FROM (SELECT concat('ab', toString(intDiv(number, 4) % 3), 'cd') AS h FROM numbers(2000))
SETTINGS max_block_size = 2000;

-- Almost no row matches, so the pre-check engages, while the sparse matching rows still go through the cache.
SELECT countIf(replaceRegexpAll(h, '^ab([0-9]+)', '<\\1>') != replaceRegexpAll(h, materialize('^ab([0-9]+)'), '<\\1>'))
FROM (SELECT if(number % 100 = 50, concat('ab', toString(number % 7), 'cd'), concat('xx', toString(number % 13), 'yy')) AS h FROM numbers(5000))
SETTINGS max_block_size = 5000;

-- A rejecting distinct prefix followed by a matching distinct suffix: the guards disable the cache and engage
-- the pre-check on the prefix, and the re-evaluated match ratio turns the pre-check off again on the suffix.
SELECT countIf(replaceRegexpOne(h, '^ab([0-9]+)', '<\\1>') != replaceRegexpOne(h, materialize('^ab([0-9]+)'), '<\\1>'))
FROM (SELECT if(number < 1000, concat('xx', toString(number), 'yy'), concat('ab', toString(number), 'cd')) AS h FROM numbers(4000))
SETTINGS max_block_size = 4000;

-- The values a repeat is expected to produce, so that the cases above cannot pass by both sides being wrong.
SELECT DISTINCT replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1')
FROM (SELECT concat('ab', toString(number % 3), 'cd', toString(number % 2)) AS h FROM numbers(100))
ORDER BY 1;
