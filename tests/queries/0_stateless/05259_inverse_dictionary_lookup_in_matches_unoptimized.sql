-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dictionary is not available on the replicas.

-- The rewrite of `dictGet(...) [NOT] IN (...)` by `optimize_inverse_dictionary_lookup` must give the same
-- results as the original predicate, including for keys missing from the dictionary (which get the attribute
-- `DEFAULT`), for a `DEFAULT` that is in the list or not, and for list elements not representable in the attribute type.

SET enable_analyzer = 1;
SET rewrite_in_to_join = 0;

DROP DICTIONARY IF EXISTS d_05259;
DROP DICTIONARY IF EXISTS c_05259;
DROP TABLE IF EXISTS d_src_05259;
DROP TABLE IF EXISTS c_src_05259;
DROP TABLE IF EXISTS t_05259;

CREATE TABLE d_src_05259 (k UInt64, s String, n UInt64) ENGINE = MergeTree ORDER BY k;
-- Key 4 stores the `DEFAULT` of `s` explicitly, key 3 and key 5 store the `DEFAULT` of `n`.
INSERT INTO d_src_05259 VALUES (1, 'a', 1), (2, 'b', 2), (3, 'a', 7), (4, 'none', 3), (5, 'c', 7);

CREATE DICTIONARY d_05259 (k UInt64, s String DEFAULT 'none', n UInt64 DEFAULT 7)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'd_src_05259')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE c_src_05259 (k1 UInt64, k2 String, v String) ENGINE = MergeTree ORDER BY k1;
INSERT INTO c_src_05259 VALUES (1, 'x', 'p'), (2, 'x', 'q'), (3, 'y', 'p'), (4, 'x', 'dv');

CREATE DICTIONARY c_05259 (k1 UInt64, k2 String, v String DEFAULT 'dv')
PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'c_src_05259')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

-- Keys 0 and 6..9 are missing from both dictionaries.
CREATE TABLE t_05259 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05259 SELECT number FROM numbers(10);

SELECT 'dictGetString(\'d_05259\', \'s\', k) IN (\'a\', \'b\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('a', 'b')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('a', 'b') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('a', 'b') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('a', 'b') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('a', 'b') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetString(\'d_05259\', \'s\', k) IN (\'a\', \'none\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('a', 'none')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('a', 'none') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('a', 'none') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('a', 'none') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('a', 'none') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetString(\'d_05259\', \'s\', k) IN (\'none\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('none')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('none') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('none') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('none') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('none') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetString(\'d_05259\', \'s\', k) IN (\'zzz\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('zzz')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('zzz') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) IN ('zzz') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('zzz') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) IN ('zzz') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetString(\'d_05259\', \'s\', k) NOT IN (\'a\', \'b\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) NOT IN ('a', 'b')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) NOT IN ('a', 'b') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) NOT IN ('a', 'b') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) NOT IN ('a', 'b') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) NOT IN ('a', 'b') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetString(\'d_05259\', \'s\', k) NOT IN (\'a\', \'none\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) NOT IN ('a', 'none')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) NOT IN ('a', 'none') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetString('d_05259', 's', k) NOT IN ('a', 'none') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) NOT IN ('a', 'none') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetString('d_05259', 's', k) NOT IN ('a', 'none') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet(\'d_05259\', \'s\', k) IN [\'c\', \'none\']';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGet('d_05259', 's', k) IN ['c', 'none']) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('d_05259', 's', k) IN ['c', 'none'] ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('d_05259', 's', k) IN ['c', 'none'] ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGet('d_05259', 's', k) IN ['c', 'none'] AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGet('d_05259', 's', k) IN ['c', 'none'] AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) IN (1, 7)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (1, 7)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (1, 7) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (1, 7) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (1, 7) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (1, 7) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) IN (1, 2)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (1, 2)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (1, 2) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (1, 2) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (1, 2) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (1, 2) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) NOT IN (7)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (7)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (7) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (7) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) NOT IN (7) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) NOT IN (7) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) NOT IN (1, 2)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (1, 2)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (1, 2) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (1, 2) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) NOT IN (1, 2) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) NOT IN (1, 2) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) IN (7.5, 1)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (7.5, 1)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (7.5, 1) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (7.5, 1) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (7.5, 1) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (7.5, 1) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) IN (7.0, 1)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (7.0, 1)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (7.0, 1) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) IN (7.0, 1) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (7.0, 1) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) IN (7.0, 1) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetUInt64(\'d_05259\', \'n\', k) NOT IN (-1, 2)';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (-1, 2)) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (-1, 2) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGetUInt64('d_05259', 'n', k) NOT IN (-1, 2) ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) NOT IN (-1, 2) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGetUInt64('d_05259', 'n', k) NOT IN (-1, 2) AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet(\'d_05259\', \'n\', k) IN [7, 3]';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGet('d_05259', 'n', k) IN [7, 3]) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('d_05259', 'n', k) IN [7, 3] ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('d_05259', 'n', k) IN [7, 3] ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGet('d_05259', 'n', k) IN [7, 3] AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGet('d_05259', 'n', k) IN [7, 3] AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet(\'c_05259\', \'v\', (k, \'x\')) IN (\'p\', \'dv\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'dv')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'dv') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'dv') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'dv') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'dv') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet(\'c_05259\', \'v\', (k, \'x\')) IN (\'p\', \'q\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'q')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'q') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'q') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'q') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) IN ('p', 'q') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet(\'c_05259\', \'v\', (k, \'x\')) NOT IN (\'p\', \'dv\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p', 'dv')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p', 'dv') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p', 'dv') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p', 'dv') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p', 'dv') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet(\'c_05259\', \'v\', (k, \'x\')) NOT IN (\'p\')';
SELECT countIf(explain LIKE '%table_function_name: dictionary%') > 0 AS rewritten FROM (EXPLAIN QUERY TREE SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p')) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_05259 WHERE dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p') ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT groupArray(r) FROM (SELECT dictGet('c_05259', 'v', (k, 'x')) NOT IN ('p') AS r FROM t_05259 ORDER BY k) SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP DICTIONARY d_05259;
DROP DICTIONARY c_05259;
DROP TABLE d_src_05259;
DROP TABLE c_src_05259;
DROP TABLE t_05259;
