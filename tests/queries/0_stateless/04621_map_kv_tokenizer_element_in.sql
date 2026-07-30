-- m['key'] IN (v1, ..., vn) over a keyValuePairs text index is the union of the exact first-value
-- lookups m['key'] = vi. The index prunes granules and, for literal sets, is used for exact direct read:
-- the folded FUNCTION_HAS_ANY_TOKENS query answers the predicate from the index (the Map column is not
-- read on materialized parts). For parts where the index is not materialized, the virtual column's
-- default expression is rebuilt as m['key'] = v1 OR ... OR m['key'] = vn (literals that survive the
-- round-trip), so no rows are dropped. Results must equal a plain scan on every part, materialized or not.
-- A set with the empty string falls back to a scan, because m['key'] = '' is true for rows lacking the key.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;

CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_mem VALUES
    (1, map('lvl', 'err')), (2, map('lvl', 'info')), (3, map('lvl', 'warn')),
    (4, map('svc', 'api')), (5, map('lvl', 'debug')), (6, map('lvl', 'err')),
    (7, map('k', 'a', 'k', 'b'));    -- duplicate key: first value 'a'
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- index (granule pruning) == plain scan --';
SELECT id FROM t_mem WHERE m['lvl'] IN ('err', 'warn') ORDER BY id;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 1;

SELECT '-- direct read IS used for the literal IN set form (1) --';
SELECT 'direct read', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') SETTINGS query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%__text_index_%';

SELECT '-- direct-read path == plain scan --';
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- absent values match nothing --';
SELECT count() FROM t_idx WHERE m['lvl'] IN ('nope', 'none') SETTINGS use_skip_indexes = 1;

SELECT '-- empty string in the set: rows without the key match --';
SELECT id FROM t_mem WHERE m['lvl'] IN ('err', '') ORDER BY id;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', '') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- duplicate key: first-value (arrayElement) semantics, k=(a,b) matches on a, not b --';
SELECT id FROM t_idx WHERE m['k'] IN ('a') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_idx WHERE m['k'] IN ('b') SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_mem;
DROP TABLE t_idx;

SELECT '-- regression: a part where the index is not materialized must not drop rows --';
DROP TABLE IF EXISTS t_mix;
CREATE TABLE t_mix (id UInt64, m Map(String, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mix VALUES (10, map('lvl', 'err')), (11, map('lvl', 'warn')), (12, map('lvl', 'info'));   -- part left non-materialized
ALTER TABLE t_mix ADD INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1;
INSERT INTO t_mix VALUES (1, map('lvl', 'err')), (2, map('lvl', 'warn')), (3, map('lvl', 'debug'));      -- part materialized (index built on insert)
SELECT id FROM t_mix WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;
SELECT '-- regression (subcolumn form): non-materialized part must not drop rows --';
SELECT id FROM t_mix WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 1;
DROP TABLE t_mix;
