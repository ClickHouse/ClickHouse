DROP TABLE IF EXISTS tab;

SELECT 'Text index';
CREATE TABLE tab (a Array(String), INDEX idx a TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (['x']), (['y']), (['x', 'z']);
SELECT count() FROM tab WHERE a = ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE a = ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE a != ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE a != ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE startsWith(a, ['x']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE startsWith(a, ['x']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE endsWith(a, ['x']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE endsWith(a, ['x']) SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'tokenbf_v1';
CREATE TABLE tab (a Array(String), INDEX idx a TYPE tokenbf_v1(256, 2, 0))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (['x']), (['y']), (['x', 'z']);
SELECT count() FROM tab WHERE a = ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE a = ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE a != ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE a != ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE startsWith(a, ['x']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE startsWith(a, ['x']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE endsWith(a, ['x']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE endsWith(a, ['x']) SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'ngrambf_v1';
CREATE TABLE tab (a Array(String), INDEX idx a TYPE ngrambf_v1(3, 256, 2, 0))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (['x']), (['y']), (['x', 'z']);
SELECT count() FROM tab WHERE a = ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE a = ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE a != ['x'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE a != ['x'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE startsWith(a, ['x']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE startsWith(a, ['x']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE endsWith(a, ['x']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE endsWith(a, ['x']) SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
