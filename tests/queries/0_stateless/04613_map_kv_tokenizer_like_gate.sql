-- The keyValuePairs LIKE rewrites (mapContainsValueLike, mapContainsKeyLike, m['key'] LIKE) must obey
-- the same dictionary-scan contract as the non-map LIKE path: they use the index only when
-- use_text_index_like_evaluation_by_dictionary_scan is on and the needle is at least
-- text_index_like_min_pattern_length long. Otherwise the query falls back to a full scan (still correct).

DROP TABLE IF EXISTS t_map_kv_like;
CREATE TABLE t_map_kv_like
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_map_kv_like VALUES (1, {'level':'error'}), (2, {'level':'info'}), (3, {'svc':'errand'});

-- Results are independent of the dictionary-scan setting: only 'error' (row 1) contains 'rror'.
SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rror%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rror%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

-- The __text_index_ virtual column (direct read) is present only when the dictionary scan is enabled
-- and the needle is long enough. query_plan_direct_read_from_text_index is pinned so the assertion
-- tests the LIKE gate, not the direct-read toggle.
SELECT 'valueLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 3, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'valueLike off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'valueLike short', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rr%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

SELECT 'keyLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsKeyLike(m, '%evel%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 3, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'keyLike off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsKeyLike(m, '%evel%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

SELECT 'elementLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE '%rror%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 3, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'elementLike off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE '%rror%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

DROP TABLE t_map_kv_like;
