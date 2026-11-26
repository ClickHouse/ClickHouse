-- Tags: no-parallel-replicas

-- With constant folding every partition gets its own condition. hasTokenLike and hasTokenPrefix must share
-- the automata and their budget across them, so the number of built automata does not grow with the partitions.

DROP TABLE IF EXISTS tab_dfa_budget;
CREATE TABLE tab_dfa_budget (id UInt64, body String, INDEX idx body TYPE text(tokenizer = array, dictionary_block_size = 128))
ENGINE = MergeTree PARTITION BY id % 8 ORDER BY id;
INSERT INTO tab_dfa_budget SELECT number, concat('unrelated', toString(number), 'error') FROM numbers(8000);

SELECT count() FROM tab_dfa_budget WHERE (hasTokenLike(body, 'service%a______1') OR hasTokenLike(body, 'service%a______2') OR hasTokenLike(body, 'service%a______3') OR hasTokenLike(body, 'service%a______4') OR hasTokenLike(body, 'service%a______5') OR hasTokenLike(body, 'service%a______6') OR hasTokenLike(body, 'service%a______7') OR hasTokenLike(body, 'service%a______8') OR hasTokenLike(body, 'service%a______9') OR hasTokenLike(body, 'service%a______10') OR hasTokenLike(body, 'service%a______11') OR hasTokenLike(body, 'service%a______12') OR hasTokenLike(body, 'service%a______13') OR hasTokenLike(body, 'service%a______14') OR hasTokenLike(body, 'service%a______15') OR hasTokenLike(body, 'service%a______16') OR hasTokenLike(body, 'service%a______17') OR hasTokenLike(body, 'service%a______18') OR hasTokenLike(body, 'service%a______19') OR hasTokenLike(body, 'service%a______20') OR hasTokenLike(body, 'service%a______21') OR hasTokenLike(body, 'service%a______22') OR hasTokenLike(body, 'service%a______23') OR hasTokenLike(body, 'service%a______24') OR hasTokenLike(body, 'service%a______25') OR hasTokenLike(body, 'service%a______26') OR hasTokenLike(body, 'service%a______27') OR hasTokenLike(body, 'service%a______28') OR hasTokenLike(body, 'service%a______29') OR hasTokenLike(body, 'service%a______30') OR hasTokenLike(body, 'service%a______31') OR hasTokenLike(body, 'service%a______32') OR hasTokenLike(body, 'service%a______33') OR hasTokenLike(body, 'service%a______34') OR hasTokenLike(body, 'service%a______35') OR hasTokenLike(body, 'service%a______36') OR hasTokenLike(body, 'service%a______37') OR hasTokenLike(body, 'service%a______38') OR hasTokenLike(body, 'service%a______39') OR hasTokenLike(body, 'service%a______40') OR hasTokenPrefix(body, 'unrelated1error')) AND id % 8 < 100
SETTINGS use_constant_folding_in_index_analysis = 1, use_query_condition_cache = 0, max_threads = 1, log_comment = 'hasTokenPrefixLikeMatch_dfa_budget';

DROP TABLE tab_dfa_budget;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['TextIndexDictionaryDFABuilds'] BETWEEN 1 AND 41
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'hasTokenPrefixLikeMatch_dfa_budget'
    AND event_date >= yesterday() AND event_time >= now() - 600;
