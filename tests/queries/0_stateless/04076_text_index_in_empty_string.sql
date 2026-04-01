-- Tags: no-fasttest, no-parallel-replicas

DROP TABLE IF EXISTS t_text_idx_in_empty;

CREATE TABLE t_text_idx_in_empty (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=1;
INSERT INTO t_text_idx_in_empty VALUES (1, 'hello world'), (2, 'foo bar'), (3, ''), (4, 'hello foo');

-- IN with empty string should match rows 1 and 3
SELECT id FROM t_text_idx_in_empty WHERE val IN ('hello world', '') ORDER BY id;
SELECT id FROM t_text_idx_in_empty WHERE val IN ('hello world', '') ORDER BY id SETTINGS use_skip_indexes=0;

-- IN with only empty string should match row 3
SELECT id FROM t_text_idx_in_empty WHERE val IN ('') ORDER BY id;
SELECT id FROM t_text_idx_in_empty WHERE val IN ('') ORDER BY id SETTINGS use_skip_indexes=0;

-- Combined: IN with empty string AND another condition
SELECT id FROM t_text_idx_in_empty WHERE val IN ('hello world', '') AND hasAllTokens(val, 'hello') ORDER BY id;
SELECT id FROM t_text_idx_in_empty WHERE val IN ('hello world', '') AND hasAllTokens(val, 'hello') ORDER BY id SETTINGS use_skip_indexes=0;

DROP TABLE IF EXISTS t_text_idx_in_empty;

-- With preprocessor
CREATE TABLE t_text_idx_in_empty (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=1;
INSERT INTO t_text_idx_in_empty VALUES (1, 'HELLO WORLD'), (2, 'FOO BAR'), (3, ''), (4, 'HELLO FOO');

SELECT id FROM t_text_idx_in_empty WHERE val IN ('HELLO WORLD', '') ORDER BY id;
SELECT id FROM t_text_idx_in_empty WHERE val IN ('HELLO WORLD', '') ORDER BY id SETTINGS use_skip_indexes=0;

DROP TABLE IF EXISTS t_text_idx_in_empty;
