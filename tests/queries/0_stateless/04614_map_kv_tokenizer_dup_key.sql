-- Captures the behavior of m['key'] equality/inequality predicates (and their NOT forms) over a Map
-- that contains a duplicate key, with and without a keyValuePairs text index. m['key'] returns the
-- first value for the key, while the index stores every (key, value) pair; this test pins the current
-- behavior of both engines so any change to how the index answers these predicates is visible.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;

CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_mem VALUES (1, map('k', 'a', 'k', 'b'));
INSERT INTO t_idx VALUES (1, map('k', 'a', 'k', 'b'));

SELECT '======== Memory (no index) ========';
SELECT 'SECOND ARG';
SELECT '=============1';
SELECT * FROM t_mem WHERE m['k'] = 'b';
SELECT '=============2';
SELECT * FROM t_mem WHERE NOT m['k'] != 'b';
SELECT '=============3';
SELECT * FROM t_mem WHERE m['k'] != 'b';
SELECT '=============4';
SELECT * FROM t_mem WHERE NOT m['k'] = 'b';
SELECT 'FIRST ARG';
SELECT '=============1';
SELECT * FROM t_mem WHERE m['k'] = 'a';
SELECT '=============2';
SELECT * FROM t_mem WHERE NOT m['k'] != 'a';
SELECT '=============3';
SELECT * FROM t_mem WHERE m['k'] != 'a';
SELECT '=============4';
SELECT * FROM t_mem WHERE NOT m['k'] = 'a';

SELECT '======== MergeTree + keyValuePairs index ========';
SELECT 'SECOND ARG';
SELECT '=============1';
SELECT * FROM t_idx WHERE m['k'] = 'b';
SELECT '=============2';
SELECT * FROM t_idx WHERE NOT m['k'] != 'b';
SELECT '=============3';
SELECT * FROM t_idx WHERE m['k'] != 'b';
SELECT '=============4';
SELECT * FROM t_idx WHERE NOT m['k'] = 'b';
SELECT 'FIRST ARG';
SELECT '=============1';
SELECT * FROM t_idx WHERE m['k'] = 'a';
SELECT '=============2';
SELECT * FROM t_idx WHERE NOT m['k'] != 'a';
SELECT '=============3';
SELECT * FROM t_idx WHERE m['k'] != 'a';
SELECT '=============4';
SELECT * FROM t_idx WHERE NOT m['k'] = 'a';

DROP TABLE t_mem;
DROP TABLE t_idx;
