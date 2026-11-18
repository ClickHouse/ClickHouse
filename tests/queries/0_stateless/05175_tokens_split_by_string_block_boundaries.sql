-- The splitByString tokenizer scans strings in 16-byte blocks: separators crossing block boundaries,
-- separators that are prefixes of one another, and unfinished separators at the end of the string.

SELECT '-- empty and separator-only strings';
SELECT tokens('', 'splitByString', [', ']);
SELECT tokens(', , ', 'splitByString', [', ']);
SELECT tokens(', ,', 'splitByString', [', ']);
SELECT tokens('a', 'splitByString', [', ']);

SELECT '-- separator crossing, starting at, and ending at the block boundary';
SELECT tokens('aaaaaaaaaaaaaaa, b', 'splitByString', [', ']);
SELECT tokens('aaaaaaaaaaaaaaaa, b', 'splitByString', [', ']);
SELECT tokens('aaaaaaaaaaaaaa, b', 'splitByString', [', ']);
SELECT tokens('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, b', 'splitByString', [', ']);

SELECT '-- candidate first byte that does not complete a separator';
SELECT tokens('aaaaaaaaaaaaaaa=>b', 'splitByString', ['=>']);
SELECT tokens('aaaaaaaaaaaaaaa=b=>c', 'splitByString', ['=>']);
SELECT tokens('a=b=c', 'splitByString', ['=>']);
SELECT tokens('a=>b=', 'splitByString', ['=>']);
SELECT tokens('a=>b=>', 'splitByString', ['=>']);
SELECT tokens('aaaaaaaaaaaaaaa=', 'splitByString', ['=>']);

SELECT '-- separators that are prefixes of one another: the first listed one wins';
SELECT tokens('a, b,c', 'splitByString', [',', ', ']);
SELECT tokens('a, b,c', 'splitByString', [', ', ',']);
SELECT tokens('a===b==c=d', 'splitByString', ['==', '===', '=']);
SELECT tokens('a===b==c=d', 'splitByString', ['=', '==', '===']);
SELECT tokens('a===b==c=d', 'splitByString', ['===', '==']);

SELECT '-- strings of exactly one and two blocks';
SELECT tokens('0123456789abcde ', 'splitByString', [' ']);
SELECT tokens('0123456789abcde ', 'splitByString', ['e ']);
SELECT tokens(' 123456789abcdef', 'splitByString', [' 1', 'ef']);
SELECT tokens('0123456789abcdef0123456789abcdef', 'splitByString', ['f0', 'ef']);
SELECT tokens('0123456789abcdef0123456789abcdef', 'splitByString', ['f']);

SELECT '-- runs of separators and tokens longer than a block';
SELECT tokens(concat('a', repeat(', ', 20), 'b'), 'splitByString', [', ']);
SELECT tokens(concat('a', repeat(',', 20), 'b'), 'splitByString', [',']);
SELECT tokens(concat(repeat('x', 40), ' ', repeat('y', 17), ' z'), 'splitByString', [' ']);
SELECT tokens(concat(repeat('x', 40), ', ', repeat('y', 17), ', z'), 'splitByString', [', ']);
SELECT length(tokens(repeat('ab, ', 100), 'splitByString', [', '])), arrayDistinct(tokens(repeat('ab, ', 100), 'splitByString', [', ']));
SELECT length(tokens(repeat('ab, cd. ', 100), 'splitByString', [', ', '. ', ' '])), arrayDistinct(tokens(repeat('ab, cd. ', 100), 'splitByString', [', ', '. ', ' ']));

SELECT '-- non-ASCII separators';
SELECT tokens('aé bé c', 'splitByString', ['é ']);
SELECT tokens('a→b→→c→', 'splitByString', ['→']);
SELECT tokens('a→b→→c→', 'splitByString', ['→→', '→']);
SELECT tokens('ααααααααααααααα→β', 'splitByString', ['→']);

SELECT '-- log-like lines';
SELECT tokens('2024-01-01 12:00:00.123 [thread-7] INFO user_id=42 method=GET path=/api/v1/items/ABC status=200 ua="Mozilla/5.0 (X11; Linux x86_64)" ok', 'splitByString', [' ', '=', ',', '"']);
SELECT tokens('key="v1", other="v2", last=3', 'splitByString', [', ', '="', '"', '=']);
SELECT tokens('Many years later, as he faced the firing squad, Colonel Aureliano Buendia was to remember that distant afternoon. 42', 'splitByString', [', ', '. ', ' ']);

SELECT '-- the same tokenizer through a text index';
DROP TABLE IF EXISTS t_split_by_string_blocks;
CREATE TABLE t_split_by_string_blocks
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByString([', ', '. ', ' ']))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_split_by_string_blocks VALUES
    (1, 'aaaaaaaaaaaaaaa, b'),
    (2, 'aaaaaaaaaaaaaaaa. c'),
    (3, 'aaaaaaaaaaaaaa, d'),
    (4, 'e, f. g h'),
    (5, ', , , ');

SELECT groupArray(id) FROM t_split_by_string_blocks WHERE hasAnyTokens(s, ['b', 'c', 'd']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM t_split_by_string_blocks WHERE hasAllTokens(s, ['aaaaaaaaaaaaaaa', 'b']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM t_split_by_string_blocks WHERE hasAnyTokens(s, ['g', 'h']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM t_split_by_string_blocks WHERE hasAnyTokens(s, ['aaaaaaaaaaaaaaaa']) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_split_by_string_blocks;
