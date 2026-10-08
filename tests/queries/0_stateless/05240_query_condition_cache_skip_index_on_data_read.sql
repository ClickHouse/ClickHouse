-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the reuse checks assert a QueryConditionCacheHits on the instance-wide query condition cache;
-- a sibling test's SYSTEM DROP QUERY CONDITION CACHE could evict the entries between the two queries.
-- Tag no-parallel-replicas: parallel replicas relocate index analysis and the query condition cache writes.

-- Skip indexes applied at data read time (use_skip_indexes_on_data_read = 1, the default) record the granules they
-- exclude in the query condition cache, under the same skip-index-profiled key as index analysis. A repeated query
-- then drops these granules (here: whole parts) before reading instead of evaluating the index on them again.
-- The queries read columns (sum(id), groupArray(s)) so that they are not answered from the text index alone.

SET use_query_condition_cache = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, auto_statistics_types = '';

SYSTEM STOP MERGES tab;
INSERT INTO tab SELECT number, 'hay' FROM numbers(1000);
INSERT INTO tab SELECT number + 1000, 'hay' FROM numbers(1000);
INSERT INTO tab SELECT number + 2000, if(number = 500, 'needle', 'hay') FROM numbers(1000);

SELECT 'token_first', sum(id) FROM tab WHERE hasToken(s, 'needle') SETTINGS log_comment = '05240_qcc_token_first';
SELECT 'token_repeat', sum(id) FROM tab WHERE hasToken(s, 'needle') SETTINGS log_comment = '05240_qcc_token_repeat';
SELECT 'like_first', sum(id) FROM tab WHERE s LIKE '%needl%' SETTINGS log_comment = '05240_qcc_like_first';
SELECT 'like_repeat', sum(id) FROM tab WHERE s LIKE '%needl%' SETTINGS log_comment = '05240_qcc_like_repeat';

DROP TABLE tab;

-- The entries are keyed by the skip indexes that ran (#108519): a text index with a preprocessor legitimately
-- diverges from the row-level predicate (row 'a b' is indexed as the token 'ab'), so its exclusion must not be
-- served to a query that disabled or ignored the index. The row-level answer for those queries is ['a b'].
CREATE TABLE tab
(
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(s, ' ', ''))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, auto_statistics_types = '';

INSERT INTO tab VALUES ('zzz'), ('a b');

SELECT 'index', groupArray(s) FROM tab WHERE hasToken(s, 'a') SETTINGS log_comment = '05240_qcc_index';
SELECT 'index_repeat', groupArray(s) FROM tab WHERE hasToken(s, 'a') SETTINGS log_comment = '05240_qcc_index_repeat';
SELECT 'skip_indexes_off', groupArray(s) FROM tab WHERE hasToken(s, 'a') SETTINGS use_skip_indexes = 0;
SELECT 'ignore_index', groupArray(s) FROM tab WHERE hasToken(s, 'a') SETTINGS ignore_data_skipping_indices = 'idx';

DROP TABLE tab;

SYSTEM FLUSH LOGS query_log;

SELECT replaceOne(log_comment, '05240_qcc_', ''), ProfileEvents['QueryConditionCacheHits'] > 0 AS cache_hit, ProfileEvents['SelectedParts'] AS parts
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05240_qcc_token_first', '05240_qcc_token_repeat', '05240_qcc_like_first', '05240_qcc_like_repeat', '05240_qcc_index', '05240_qcc_index_repeat')
ORDER BY event_time_microseconds;
