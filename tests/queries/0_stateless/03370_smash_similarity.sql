-- Test basic functionality of smashSimilarity and smashSimilarityUTF8
SELECT smashSimilarity('test', 'test') AS exact_match;
SELECT smashSimilarityUTF8('тест', 'тест') AS exact_match_utf8;

-- Empty strings edge cases
SELECT smashSimilarity('', '') AS empty_strings;
SELECT smashSimilarity('', 'test') AS empty_first;
SELECT smashSimilarity('test', '') AS empty_second;

-- Single word with small variations
SELECT smashSimilarity('hello', 'helo') AS small_change;
SELECT smashSimilarity('hello', 'hello!') AS punctuation_change;
SELECT smashSimilarity('hello', 'Hello') AS case_change;

-- Spaces and word segmentation
SELECT smashSimilarity('hello world', 'helloworld') AS space_removed;
SELECT smashSimilarity('hello world', 'hello  world') AS extra_space;
SELECT smashSimilarity('hello world', 'world hello') AS word_order_changed;

-- Phrase variations
SELECT smashSimilarity('the quick brown fox', 'quick brown fox the') AS word_order_phrase;
SELECT smashSimilarity('the quick brown fox', 'the quik brown fox') AS typo_in_phrase;
SELECT smashSimilarity('the quick brown fox jumps', 'the quick fox') AS missing_words;

-- Completely different strings
SELECT smashSimilarity('completely different', 'nothing in common') AS no_similarity;

-- -- Test UTF-8 specific cases
SELECT smashSimilarityUTF8('привет мир', 'привет') AS utf8_substring;
SELECT smashSimilarityUTF8('привет мир', 'мир привет') AS utf8_word_swap;
SELECT smashSimilarityUTF8('кошка и собака', 'собака и кот') AS utf8_similar;

-- Comparison with other string distance functions
SELECT smashSimilarity('hello world', 'hello there') AS smash;

-- Typos ans abbreviations
SELECT smashSimilarity('sro', 'school resource officer') AS smash;
SELECT smashSimilarity('assistant park manager', 'aspmng') AS smash;
SELECT smashSimilarity('assistant ark manager', 'aspmng') AS smash;
SELECT smashSimilarity('text with tyypo', 'text with rypo') AS smash;
SELECT smashSimilarity('deputy marshall', 'dptymrsl') AS smash;
SELECT smashSimilarity('united nations', 'un') AS smash;