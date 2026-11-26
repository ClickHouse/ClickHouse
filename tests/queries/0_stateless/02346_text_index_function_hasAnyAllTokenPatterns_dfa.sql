-- Tags: no-parallel-replicas

-- hasAnyTokenLike and hasAllTokenLike share the DFAs and their budget across the conditions of one query,
-- and a selective prefix pattern still seeks in a large dictionary.

DROP TABLE IF EXISTS tab_dfa_budget;
CREATE TABLE tab_dfa_budget (id UInt64, body String, INDEX idx body TYPE text(tokenizer = array, dictionary_block_size = 128))
ENGINE = MergeTree PARTITION BY id % 8 ORDER BY id;
INSERT INTO tab_dfa_budget SELECT number, concat('unrelated', toString(number), 'error') FROM numbers(8000);

-- Every partition gets its own condition, the number of built automata must not grow with the partitions.
SELECT count() FROM tab_dfa_budget WHERE (hasAnyTokenLike(body, 'service%a______1') OR hasAnyTokenLike(body, 'service%a______2') OR hasAnyTokenLike(body, 'service%a______3') OR hasAnyTokenLike(body, 'service%a______4') OR hasAnyTokenLike(body, 'service%a______5') OR hasAnyTokenLike(body, 'service%a______6') OR hasAnyTokenLike(body, 'service%a______7') OR hasAnyTokenLike(body, 'service%a______8') OR hasAnyTokenLike(body, 'service%a______9') OR hasAnyTokenLike(body, 'service%a______10') OR hasAnyTokenLike(body, 'service%a______11') OR hasAnyTokenLike(body, 'service%a______12') OR hasAnyTokenLike(body, 'service%a______13') OR hasAnyTokenLike(body, 'service%a______14') OR hasAnyTokenLike(body, 'service%a______15') OR hasAnyTokenLike(body, 'service%a______16') OR hasAnyTokenLike(body, 'service%a______17') OR hasAnyTokenLike(body, 'service%a______18') OR hasAnyTokenLike(body, 'service%a______19') OR hasAnyTokenLike(body, 'service%a______20') OR hasAnyTokenLike(body, 'service%a______21') OR hasAnyTokenLike(body, 'service%a______22') OR hasAnyTokenLike(body, 'service%a______23') OR hasAnyTokenLike(body, 'service%a______24') OR hasAnyTokenLike(body, 'service%a______25') OR hasAnyTokenLike(body, 'service%a______26') OR hasAnyTokenLike(body, 'service%a______27') OR hasAnyTokenLike(body, 'service%a______28') OR hasAnyTokenLike(body, 'service%a______29') OR hasAnyTokenLike(body, 'service%a______30') OR hasAnyTokenLike(body, 'service%a______31') OR hasAnyTokenLike(body, 'service%a______32') OR hasAnyTokenLike(body, 'service%a______33') OR hasAnyTokenLike(body, 'service%a______34') OR hasAnyTokenLike(body, 'service%a______35') OR hasAnyTokenLike(body, 'service%a______36') OR hasAnyTokenLike(body, 'service%a______37') OR hasAnyTokenLike(body, 'service%a______38') OR hasAnyTokenLike(body, 'service%a______39') OR hasAnyTokenLike(body, 'service%a______40') OR hasAllTokenLike(body, ['unrelated1error'])) AND id % 8 < 100
SETTINGS use_constant_folding_in_index_analysis = 1, use_query_condition_cache = 0, max_threads = 1, log_comment = 'hasAnyAllTokenPatterns_dfa_budget';

DROP TABLE tab_dfa_budget;

DROP TABLE IF EXISTS tab_dfa_seek;
CREATE TABLE tab_dfa_seek (id UInt64, body String, INDEX idx body TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_dfa_seek SELECT number, if(number = 123456, 'target_x', concat('token', toString(number))) FROM numbers(200000);

-- 200000 tokens are more than text_index_like_max_matched_tokens, but the pattern seeks to its only token.
SELECT count() FROM tab_dfa_seek WHERE hasAnyTokenLike(body, 'target_%')
SETTINGS use_query_condition_cache = 0, log_comment = 'hasAnyAllTokenPatterns_dfa_seek_any';
SELECT count() FROM tab_dfa_seek WHERE hasAllTokenLike(body, ['target_%'])
SETTINGS use_query_condition_cache = 0, log_comment = 'hasAnyAllTokenPatterns_dfa_seek_all';

DROP TABLE tab_dfa_seek;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TextIndexDictionaryDFABuilds'] BETWEEN 1 AND 41
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'hasAnyAllTokenPatterns_dfa_budget'
    AND event_date >= yesterday() AND event_time >= now() - 600;
SELECT log_comment, ProfileEvents['TextIndexDiscardPatternScan'], read_rows < 200000
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment LIKE 'hasAnyAllTokenPatterns_dfa_seek_%'
    AND event_date >= yesterday() AND event_time >= now() - 600
ORDER BY log_comment;
