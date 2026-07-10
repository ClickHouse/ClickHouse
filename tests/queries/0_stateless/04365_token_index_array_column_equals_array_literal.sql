-- Whole-array comparison (a = [...]) on an Array(String) column that carries a token-family
-- skip index (tokenbf_v1 / ngrambf_v1 / text) used to throw BAD_GET while building the index
-- condition, making a valid query unexecutable. It must instead run normally: the comparison
-- is simply not an atom the index can use, so it falls back to a full scan. Results must match
-- use_skip_indexes = 0. Issue #110038.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_tokenbf;
CREATE TABLE t_tokenbf (a Array(String), INDEX idx a TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tokenbf VALUES (['x']), (['y']), (['x', 'z']);
SELECT count() FROM t_tokenbf WHERE a = ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_tokenbf WHERE a = ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tokenbf WHERE a != ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_tokenbf WHERE a IN (['x'], ['q']) SETTINGS use_skip_indexes = 1;
-- The index must still be usable for the functions it supports.
SELECT count() FROM t_tokenbf WHERE has(a, 'x') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_tokenbf WHERE hasAll(a, ['x']) SETTINGS use_skip_indexes = 1;
DROP TABLE t_tokenbf;

DROP TABLE IF EXISTS t_ngrambf;
CREATE TABLE t_ngrambf (a Array(String), INDEX idx a TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ngrambf VALUES (['x']), (['y']), (['x', 'z']);
SELECT count() FROM t_ngrambf WHERE a = ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ngrambf WHERE a = ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngrambf WHERE a != ['x'] SETTINGS use_skip_indexes = 1;
DROP TABLE t_ngrambf;

DROP TABLE IF EXISTS t_text;
CREATE TABLE t_text (a Array(String), INDEX idx a TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_text VALUES (['x']), (['y']), (['x', 'z']);
SELECT count() FROM t_text WHERE a = ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_text WHERE a = ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text WHERE a != ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_text WHERE hasAny(a, ['x']) SETTINGS use_skip_indexes = 1;
DROP TABLE t_text;
