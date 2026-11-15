-- Index construction uses `forEachToken`; string search needles use `nextInString`.
-- Check both against scans with the index disabled, at every block alignment.
-- The tokenizer argument is passed explicitly, so that the indexed and non-indexed queries evaluate the same predicate.
DROP TABLE IF EXISTS byte_tokenizer_corners;
CREATE TABLE byte_tokenizer_corners
(
    id UInt64,
    non_alpha String,
    single_byte String,
    multi_byte String,
    INDEX non_alpha_idx non_alpha TYPE text(tokenizer = splitByNonAlpha),
    INDEX single_byte_idx single_byte TYPE text(tokenizer = splitByString(['|', '\0'])),
    INDEX multi_byte_idx multi_byte TYPE text(tokenizer = splitByString(['ab', 'aba', '::']))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO byte_tokenizer_corners
SELECT number,
    concat(repeat('_', number), repeat('x', number), '_hit_é🙂_tail_\xff\x80'),
    concat(repeat('|', number), repeat('x', number), '|hit\0é🙂|tail|\xff\x80'),
    concat(repeat('::', number), repeat('x', number), 'abahit::é🙂abtailab\xff\x80')
FROM numbers(65);
INSERT INTO byte_tokenizer_corners VALUES
    (65, '', '', ''),
    (66, '________________', '||||||||||||||||', 'abababababababab'),
    (67, 'hit', 'hit', 'hit'),
    (68, 'tail', 'tail', 'tail'),
    (69, 'hitter', 'hitter', 'hitter');

SELECT 'without indexes';
SELECT count(), sum(id) FROM byte_tokenizer_corners WHERE hasAllTokens(non_alpha, 'hit_é🙂_tail_\xff\x80') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM byte_tokenizer_corners WHERE hasAnyTokens(non_alpha, 'hitter_hit') AND NOT hasAllTokens(non_alpha, ['hit', 'tail']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM byte_tokenizer_corners WHERE hasAnyTokens(non_alpha, ['hi', 'tai', 'é']) SETTINGS use_skip_indexes = 0;

SELECT count(), sum(id) FROM byte_tokenizer_corners WHERE hasAllTokens(single_byte, 'hit\0é🙂|tail|\xff\x80', 'splitByString([\'|\', \'\0\'])') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM byte_tokenizer_corners WHERE hasAnyTokens(single_byte, ['hitter', 'hit'], 'splitByString([\'|\', \'\0\'])') AND NOT hasAllTokens(single_byte, ['hit', 'tail'], 'splitByString([\'|\', \'\0\'])') SETTINGS use_skip_indexes = 0;
SELECT count() FROM byte_tokenizer_corners WHERE hasAnyTokens(single_byte, ['hi', 'tai', 'é'], 'splitByString([\'|\', \'\0\'])') SETTINGS use_skip_indexes = 0;

-- With these separator priorities, `abahit` contains the token `ahit`, not `hit`.
SELECT count(), sum(id) FROM byte_tokenizer_corners WHERE hasAllTokens(multi_byte, 'abahit::é🙂abtailab\xff\x80', 'splitByString([\'ab\', \'aba\', \'::\'])') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM byte_tokenizer_corners WHERE hasAnyTokens(multi_byte, ['hit', 'hitter'], 'splitByString([\'ab\', \'aba\', \'::\'])') SETTINGS use_skip_indexes = 0;
SELECT count() FROM byte_tokenizer_corners WHERE hasAnyTokens(multi_byte, ['hi', 'tai', 'é'], 'splitByString([\'ab\', \'aba\', \'::\'])') SETTINGS use_skip_indexes = 0;

SELECT 'forced indexes';
SELECT count(), sum(id) FROM byte_tokenizer_corners WHERE hasAllTokens(non_alpha, 'hit_é🙂_tail_\xff\x80') SETTINGS force_data_skipping_indices = 'non_alpha_idx';
SELECT arraySort(groupArray(id)) FROM byte_tokenizer_corners WHERE hasAnyTokens(non_alpha, 'hitter_hit') AND NOT hasAllTokens(non_alpha, ['hit', 'tail']) SETTINGS force_data_skipping_indices = 'non_alpha_idx';
SELECT count() FROM byte_tokenizer_corners WHERE hasAnyTokens(non_alpha, ['hi', 'tai', 'é']) SETTINGS force_data_skipping_indices = 'non_alpha_idx';

SELECT count(), sum(id) FROM byte_tokenizer_corners WHERE hasAllTokens(single_byte, 'hit\0é🙂|tail|\xff\x80', 'splitByString([\'|\', \'\0\'])') SETTINGS force_data_skipping_indices = 'single_byte_idx';
SELECT arraySort(groupArray(id)) FROM byte_tokenizer_corners WHERE hasAnyTokens(single_byte, ['hitter', 'hit'], 'splitByString([\'|\', \'\0\'])') AND NOT hasAllTokens(single_byte, ['hit', 'tail'], 'splitByString([\'|\', \'\0\'])') SETTINGS force_data_skipping_indices = 'single_byte_idx';
SELECT count() FROM byte_tokenizer_corners WHERE hasAnyTokens(single_byte, ['hi', 'tai', 'é'], 'splitByString([\'|\', \'\0\'])') SETTINGS force_data_skipping_indices = 'single_byte_idx';

SELECT count(), sum(id) FROM byte_tokenizer_corners WHERE hasAllTokens(multi_byte, 'abahit::é🙂abtailab\xff\x80', 'splitByString([\'ab\', \'aba\', \'::\'])') SETTINGS force_data_skipping_indices = 'multi_byte_idx';
SELECT arraySort(groupArray(id)) FROM byte_tokenizer_corners WHERE hasAnyTokens(multi_byte, ['hit', 'hitter'], 'splitByString([\'ab\', \'aba\', \'::\'])') SETTINGS force_data_skipping_indices = 'multi_byte_idx';
SELECT count() FROM byte_tokenizer_corners WHERE hasAnyTokens(multi_byte, ['hi', 'tai', 'é'], 'splitByString([\'ab\', \'aba\', \'::\'])') SETTINGS force_data_skipping_indices = 'multi_byte_idx';

DROP TABLE byte_tokenizer_corners;
