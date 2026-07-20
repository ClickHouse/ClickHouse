-- The keyValuePairs LIKE rewrites (mapContainsValueLike, mapContainsKeyLike, m['key'] LIKE) accelerate
-- any pattern shape (prefix, suffix, literal, substring) via a decoded-value regex match, but only when
-- the dictionary scan is enabled (use_text_index_like_evaluation_by_dictionary_scan) and the pattern's
-- longest literal run is at least text_index_like_min_pattern_length. Otherwise the query falls back to a
-- full scan (still correct).

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

-- Results are independent of the dictionary-scan setting.
-- prefix 'erro%' -> 'error' (row 1); literal 'error' -> row 1; substring '%rror%' -> row 1; key prefix 'leve%' -> rows 1,2.
SELECT 'prefix', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT 'prefix', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'literal', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'error') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT 'literal', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'error') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'keyprefix', id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT 'keyprefix', id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

-- The __text_index_ virtual column (direct read) is present for every pattern shape when the scan is
-- enabled and the literal is long enough. query_plan_direct_read_from_text_index is pinned so the
-- assertion tests the LIKE gate, not the direct-read toggle.
SELECT 'valueLike prefix on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'valueLike literal on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'error') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'valueLike substring on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'keyLike prefix on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'elementLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE '%rror%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- Disabled dictionary scan: no direct read for any shape.
SELECT 'valueLike prefix off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'keyLike prefix off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- Literal shorter than text_index_like_min_pattern_length: no direct read even with the scan enabled.
SELECT 'valueLike short', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'err%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

DROP TABLE t_map_kv_like;
