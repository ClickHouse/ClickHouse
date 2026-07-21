-- m['key'] IN (v1, ..., vn) over a keyValuePairs text index is the union of the exact first-value
-- lookups m['key'] = vi. The index answers it via direct read and granule pruning at
-- optimize_functions_to_subcolumns=0, where the accessor is arrayElement (first occurrence) — the
-- occurrence the index pins (is_rest=0). Results must equal a plain scan. A set containing the empty
-- string falls back to a scan, because m['key'] = '' is true for rows lacking the key (default value).

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

SELECT '-- m[key] IN engages direct read (sub0) --';
SELECT 'direct read', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') SETTINGS query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%__text_index_%';

SELECT '-- index == plain scan (direct read, then granule pruning) --';
SELECT id FROM t_mem WHERE m['lvl'] IN ('err', 'warn') ORDER BY id;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0, optimize_functions_to_subcolumns = 0;

SELECT '-- absent values match nothing --';
SELECT count() FROM t_idx WHERE m['lvl'] IN ('nope', 'none') SETTINGS query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- empty string in the set: falls back, still correct (rows without the key match) --';
SELECT id FROM t_mem WHERE m['lvl'] IN ('err', '') ORDER BY id;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', '') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- duplicate key: first-value (arrayElement) semantics, k=(a,b) matches on a, not b --';
SELECT id FROM t_mem WHERE m['k'] IN ('a') ORDER BY id;
SELECT id FROM t_idx WHERE m['k'] IN ('a') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;
SELECT id FROM t_idx WHERE m['k'] IN ('b') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_mem;
DROP TABLE t_idx;
