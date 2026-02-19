SET enable_analyzer = 1;
SET max_parallel_replicas = 1;
SET use_skip_indexes_on_data_read = 1;
SET allow_experimental_full_text_index = 1;

-- Tests text index creation on lower(col) and with lower-ed columns at search time

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab (text) VALUES ('Hello, world!');

SELECT count() FROM tab WHERE hasToken(text, 'Hello');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM tab WHERE hasToken(text, 'Hello') SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM tab WHERE hasToken(lower(text), lower('Hello'));

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM tab WHERE hasToken(lower(text), lower('Hello')) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM tab WHERE hasAllTokens(text, ['Hello']);

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM tab WHERE hasAllTokens(text, ['Hello']) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

DROP TABLE tab;

-- --------------------------

CREATE TABLE tab (text String, INDEX idx_text lower(text) TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab (text) VALUES ('Hello, world!');

SELECT count() FROM tab WHERE hasToken(text, 'Hello');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM tab WHERE hasToken(text, 'Hello') SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM tab WHERE hasToken(lower(text), lower('Hello'));

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM tab WHERE hasToken(lower(text), lower('Hello')) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM tab WHERE hasAllTokens(lower(text), [lower('Hello')]);

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM tab WHERE hasAllTokens(lower(text), [lower('Hello')]) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

DROP TABLE tab;
