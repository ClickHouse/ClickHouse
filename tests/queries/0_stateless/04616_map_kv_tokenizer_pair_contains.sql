-- mapContainsKeyValue / mapContainsKeyValueLike are existence predicates over map (key, value) pairs.
-- Unlike m['key'] = value (which is the first value for the key), they match any occurrence, so they
-- are well-defined for duplicate keys. The keyValuePairs index answers them (exact token for the pair,
-- dictionary scan for the LIKE form); results must equal a plain scan.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;

CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_mem VALUES (1, {'level':'error'}), (2, {'level':'info','svc':'api'}), (3, {'k':'a','k':'b'});
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- mapContainsKeyValue: indexed == Memory --';
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'svc', 'api') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'svc', 'api') ORDER BY id;
-- Duplicate key: the pair (k, b) exists (second occurrence), so existence matches row 3 —
-- this is exactly where it differs from m['k'] = 'b' (first value is 'a').
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'k', 'b') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'k', 'b') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'level', 'nope') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'nope') ORDER BY id;

SELECT '-- mapContainsKeyValueLike: indexed == Memory --';
SELECT id FROM t_mem WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValueLike(m, 'svc', 'ap%') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'svc', 'ap%') ORDER BY id;

SELECT '-- direct read engages for the pair functions (settings pinned) --';
SELECT 'kv exact', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'error') SETTINGS query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'kvLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 3, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'kvLike off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'kvLike short', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'l%', 'e%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

DROP TABLE t_mem;
DROP TABLE t_idx;
