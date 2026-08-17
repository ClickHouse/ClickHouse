-- Compound predicates over a keyValuePairs text index: AND / OR / NOT chains that mix the map
-- functions (mapContainsKey, mapContainsValue, mapContainsKeyValue, their *Like forms) with the
-- m['key'] accessor and m['key'] IN (...). The RPN built from these combinations must produce the
-- same rows as a plain scan (Memory). Each case prints the Memory result then the indexed result;
-- they must be identical. Single-predicate feature coverage is in 04726_map_kv_tokenizer_basic;
-- randomized compound fuzzing is in 04730_map_kv_tokenizer_random_compound.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;

CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO t_mem VALUES
    (1, map('level', 'error', 'svc', 'api')),
    (2, map('level', 'info',  'svc', 'web')),
    (3, map('level', 'warn',  'svc', 'api')),
    (4, map('level', 'error', 'host', 'h1')),
    (5, map('k', 'a', 'k', 'b')),          -- duplicate key, no level/svc
    (6, map('level', 'debug'));
INSERT INTO t_idx SELECT * FROM t_mem;

-- All indexed queries pin optimize_functions_to_subcolumns = 0 so the accessor keeps its arrayElement
-- first-value semantics (which the index's is_rest = 0 token matches), and use_skip_indexes = 1 so the
-- index actually participates.

SELECT '-- OR of two literal value patterns (distinct query hashes, must not be conflated) --';
SELECT id FROM t_mem WHERE mapContainsValueLike(m, 'error') OR mapContainsValueLike(m, 'web') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsValueLike(m, 'error') OR mapContainsValueLike(m, 'web') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- OR of two m[key] LIKE literals --';
SELECT id FROM t_mem WHERE m['level'] LIKE 'error' OR m['level'] LIKE 'info' ORDER BY id;
SELECT id FROM t_idx WHERE m['level'] LIKE 'error' OR m['level'] LIKE 'info' ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- AND of mapContainsKey and mapContainsValue --';
SELECT id FROM t_mem WHERE mapContainsKey(m, 'svc') AND mapContainsValue(m, 'error') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKey(m, 'svc') AND mapContainsValue(m, 'error') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- AND of mapContainsKeyValue and mapContainsKey --';
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'level', 'error') AND mapContainsKey(m, 'host') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'error') AND mapContainsKey(m, 'host') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- OR of mapContainsKey and mapContainsKeyValue --';
SELECT id FROM t_mem WHERE mapContainsKey(m, 'host') OR mapContainsKeyValue(m, 'svc', 'web') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKey(m, 'host') OR mapContainsKeyValue(m, 'svc', 'web') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- NOT of a single existence predicate --';
SELECT id FROM t_mem WHERE NOT mapContainsValue(m, 'error') ORDER BY id;
SELECT id FROM t_idx WHERE NOT mapContainsValue(m, 'error') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- AND NOT: key present but not with a given value --';
SELECT id FROM t_mem WHERE mapContainsKey(m, 'level') AND NOT mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKey(m, 'level') AND NOT mapContainsKeyValue(m, 'level', 'error') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- m[key] IN AND mapContainsKey --';
SELECT id FROM t_mem WHERE m['level'] IN ('error', 'warn') AND mapContainsKey(m, 'svc') ORDER BY id;
SELECT id FROM t_idx WHERE m['level'] IN ('error', 'warn') AND mapContainsKey(m, 'svc') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- m[key] IN OR mapContainsKeyValue --';
SELECT id FROM t_mem WHERE m['level'] IN ('debug') OR mapContainsKeyValue(m, 'svc', 'api') ORDER BY id;
SELECT id FROM t_idx WHERE m['level'] IN ('debug') OR mapContainsKeyValue(m, 'svc', 'api') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- nested (AND) OR pair function --';
SELECT id FROM t_mem WHERE (mapContainsKey(m, 'svc') AND mapContainsValue(m, 'api')) OR mapContainsKeyValue(m, 'level', 'debug') ORDER BY id;
SELECT id FROM t_idx WHERE (mapContainsKey(m, 'svc') AND mapContainsValue(m, 'api')) OR mapContainsKeyValue(m, 'level', 'debug') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- accessor equality AND existence --';
SELECT id FROM t_mem WHERE m['level'] = 'error' AND mapContainsKey(m, 'svc') ORDER BY id;
SELECT id FROM t_idx WHERE m['level'] = 'error' AND mapContainsKey(m, 'svc') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- NOT accessor equality AND key present --';
SELECT id FROM t_mem WHERE NOT (m['level'] = 'info') AND mapContainsKey(m, 'level') ORDER BY id;
SELECT id FROM t_idx WHERE NOT (m['level'] = 'info') AND mapContainsKey(m, 'level') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- key LIKE AND value LIKE (both dictionary-scan forms) --';
SELECT id FROM t_mem WHERE mapContainsKeyLike(m, 'lev%') AND mapContainsValueLike(m, '%rror%') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyLike(m, 'lev%') AND mapContainsValueLike(m, '%rror%') ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- the index participates in a compound predicate (exact + existence AND) --';
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_idx WHERE m['level'] = 'error' AND mapContainsKey(m, 'svc') SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%Name: idx%';

DROP TABLE t_mem;
DROP TABLE t_idx;
