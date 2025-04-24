-- Test basic functionality of affineGap and affineGapUTF8
SELECT affineGap('test', 'test') AS exact_match;
SELECT affineGapUTF8('тест', 'тест') AS exact_match_utf8;

-- Empty strings edge cases
SELECT affineGap('', '') AS empty_strings;
SELECT affineGap('', 'test') AS empty_first;
SELECT affineGap('test', '') AS empty_second;

-- Single word with small variations
SELECT affineGap('hello', 'helo') AS small_change;
SELECT affineGap('hello', 'hello!') AS punctuation_change;
SELECT affineGap('hello', 'Hello') AS case_change;

-- Spaces and word segmentation
SELECT affineGap('hello world', 'helloworld') AS space_removed;
SELECT affineGap('hello world', 'hello  world') AS extra_space;
SELECT affineGap('hello world', 'world hello') AS word_order_changed;

-- Phrase variations
SELECT affineGap('the quick brown fox', 'quick brown fox the') AS word_order_phrase;
SELECT affineGap('the quick brown fox', 'the quik brown fox') AS typo_in_phrase;
SELECT affineGap('the quick brown fox jumps', 'the quick fox') AS missing_words;

-- Completely different strings
SELECT affineGap('completely different', 'nothing in common') AS no_similarity;

-- Test UTF-8 specific cases
SELECT affineGapUTF8('привет мир', 'привет') AS utf8_substring;
SELECT affineGapUTF8('привет мир', 'мир привет') AS utf8_word_swap;
SELECT affineGapUTF8('кошка и собака', 'собака и кот') AS utf8_similar;

-- Comparison with other string distance functions
SELECT affineGap('hello world', 'hello there') AS affine_gap;
SELECT editDistance('hello world', 'hello there') AS levenshtein;
SELECT damerauLevenshteinDistance('hello world', 'hello there') AS damerau_levenshtein;

-- Typos and abbreviations
SELECT affineGap('sro', 'school resource officer') AS affine_gap;
SELECT affineGap('assistant park manager', 'aspmng') AS affine_gap;
SELECT affineGap('assistant ark manager', 'aspmng') AS affine_gap;
SELECT affineGap('text with tyypo', 'text with rypo') AS affine_gap;
SELECT affineGap('deputy marshall', 'dptymrsl') AS affine_gap;
SELECT affineGap('united nations', 'un') AS affine_gap;

-- Close to the limit or hit the limit
SELECT affineGap(repeat('a', 1), repeat('a', 100000)) AS affine_gap;
SELECT affineGap(repeat('a', 1000), repeat('a', 2000)) AS affine_gap;
-- SELECT affineGap(repeat('a', 1000000), repeat('abba', 2000)) AS affine_gap_timeout;

-- Test with custom parameters
SELECT affineGap('hello', 'helo') AS custom_params;
SELECT affineGap('hello', 'helo') AS custom_params;
SELECT affineGap('hello', 'helo') AS custom_params;

-- Test with UTF-8 and custom parameters
SELECT affineGapUTF8('привет', 'привт') AS custom_params_utf8;
SELECT affineGapUTF8('привет', 'привт') AS custom_params_utf8;
SELECT affineGapUTF8('привет', 'привт') AS custom_params_utf8;

-- Test with non-ASCII characters
SELECT affineGap('café', 'cafe') AS non_ascii;
SELECT affineGap('über', 'uber') AS non_ascii;
SELECT affineGap('naïve', 'naive') AS non_ascii;

-- Test with emojis
SELECT affineGapUTF8('😃🌍', '🙃😃🌑') AS emoji;
SELECT affineGapUTF8('😃🌍', '😃') AS emoji;
SELECT affineGapUTF8('😃🌍', '🌍') AS emoji;

-- Test with mixed content
SELECT affineGapUTF8('Hello 世界', 'Hello world') AS mixed_content;
SELECT affineGapUTF8('Hello 世界', 'Привет мир') AS mixed_content;
SELECT affineGapUTF8('Hello 世界', 'Hello') AS mixed_content;

-- Test with very long strings
SELECT affineGap(repeat('a', 100), repeat('a', 100)) AS long_strings;
SELECT affineGap(repeat('a', 100), repeat('b', 100)) AS long_strings;
SELECT affineGap(repeat('a', 100), repeat('a', 90)) AS long_strings;

-- Test with very different lengths
SELECT affineGap('a', repeat('a', 100)) AS different_lengths;
SELECT affineGap(repeat('a', 100), 'a') AS different_lengths;
SELECT affineGap('a', 'b') AS different_lengths;

-- Test with special characters
SELECT affineGap('hello\nworld', 'hello world') AS special_chars;
SELECT affineGap('hello\tworld', 'hello world') AS special_chars;
SELECT affineGap('hello\r\nworld', 'hello world') AS special_chars;

-- Test with very large strings (should fail)
-- SELECT affineGap(repeat('a', 1000000), repeat('a', 1000000)) AS too_large; -- { serverError TOO_LARGE_STRING_SIZE }
-- SELECT affineGapUTF8(repeat('a', 1000000), repeat('a', 1000000)) AS too_large; -- { serverError TOO_LARGE_STRING_SIZE }
