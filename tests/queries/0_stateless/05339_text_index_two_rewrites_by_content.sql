-- Two text-search rewrites in one query must not share a lambda or a literal set of another index.

DROP TABLE IF EXISTS t_post;
DROP TABLE IF EXISTS t_post_array;
DROP TABLE IF EXISTS t_pre_array;
DROP TABLE IF EXISTS t_pre_set;
DROP TABLE IF EXISTS t_mixed;

CREATE TABLE t_post (id UInt32, a String, b String,
    INDEX ia a TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(a)),
    INDEX ib b TYPE text(tokenizer = splitByNonAlpha, postprocessor = upper(b)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_post VALUES (1, 'Hello', 'World');

SELECT hasAnyTokens(a, 'HELLO'), hasAnyTokens(b, 'world') FROM t_post SETTINGS use_skip_indexes = 0;
SELECT hasAnyTokens(b, 'world'), hasAnyTokens(a, 'HELLO') FROM t_post SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_post WHERE hasAnyTokens(a, 'HELLO') AND hasAnyTokens(b, 'world') SETTINGS use_skip_indexes = 0;

-- The Array column adds a tokenizing lambda of another type.
CREATE TABLE t_post_array (id UInt32, arr Array(String), s String,
    INDEX iarr arr TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(arr)),
    INDEX is s TYPE text(tokenizer = splitByNonAlpha, postprocessor = upper(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_post_array VALUES (1, ['Hello there'], 'World');

SELECT hasAnyTokens(arr, 'HELLO'), hasAnyTokens(s, 'world') FROM t_post_array SETTINGS use_skip_indexes = 0;
SELECT hasAnyTokens(s, 'world'), hasAnyTokens(arr, 'HELLO') FROM t_post_array SETTINGS use_skip_indexes = 0;

-- A preprocessor applies only to an index-analyzed filter: an Array column wraps it into a lambda.
CREATE TABLE t_pre_array (id UInt32, a Array(String), b Array(String),
    INDEX ia a TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(a)),
    INDEX ib b TYPE text(tokenizer = splitByNonAlpha, preprocessor = upper(b)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_pre_array VALUES (1, ['Hello'], ['World']);

SELECT count() FROM t_pre_array WHERE hasAnyTokens(a, 'HELLO') AND hasAnyTokens(b, 'world')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

-- A literal set in a preprocessor; the needle differs from the row, so only the preprocessor makes them match.
CREATE TABLE t_pre_set (id UInt32, a String, b String,
    INDEX ia a TYPE text(tokenizer = splitByNonAlpha, preprocessor = if(a IN ('Hello', 'HELLO'), 'yes', 'no')),
    INDEX ib b TYPE text(tokenizer = splitByNonAlpha, preprocessor = if(b IN ('Other', 'OTHER'), 'yes', 'no')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_pre_set VALUES (1, 'Hello', 'Other');

SELECT count() FROM t_pre_set WHERE hasToken(a, 'HELLO') AND hasToken(b, 'OTHER')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_pre_set WHERE hasToken(b, 'OTHER') AND hasToken(a, 'HELLO')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

-- A postprocessor next to an Array preprocessor.
CREATE TABLE t_mixed (id UInt32, a String, b Array(String),
    INDEX ia a TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(a)),
    INDEX ib b TYPE text(tokenizer = splitByNonAlpha, preprocessor = upper(b)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_mixed VALUES (1, 'Hello', ['World']);

SELECT count() FROM t_mixed WHERE hasAnyTokens(a, 'HELLO') AND hasAnyTokens(b, 'world')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_mixed WHERE hasAnyTokens(b, 'world') AND hasAnyTokens(a, 'HELLO')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

DROP TABLE t_post;
DROP TABLE t_post_array;
DROP TABLE t_pre_array;
DROP TABLE t_pre_set;
DROP TABLE t_mixed;
