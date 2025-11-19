DROP TABLE IF EXISTS test_text_index_lower;

SET enable_analyzer = 1;
SET max_parallel_replicas = 1;
SET use_skip_indexes_on_data_read = 1;
SET allow_experimental_full_text_index = 1;

CREATE TABLE test_text_index_lower (text String, INDEX idx_text text TYPE text(tokenizer='default')) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_text_index_lower (text) VALUES ('Hello, world!');

SELECT count() FROM test_text_index_lower WHERE hasToken(text, 'Hello');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM test_text_index_lower WHERE hasToken(text, 'Hello')
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM test_text_index_lower WHERE hasToken(lower(text), lower('Hello'));

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM test_text_index_lower WHERE hasToken(lower(text), lower('Hello'))
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM test_text_index_lower WHERE searchAll(text, ['Hello']);

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM test_text_index_lower WHERE searchAll(text, ['Hello'])
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

DROP TABLE IF EXISTS test_text_index_lower;

CREATE TABLE test_text_index_lower (text String, INDEX idx_text lower(text) TYPE text(tokenizer='default')) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_text_index_lower (text) VALUES ('Hello, world!');

SELECT count() FROM test_text_index_lower WHERE hasToken(text, 'Hello');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM test_text_index_lower WHERE hasToken(text, 'Hello')
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM test_text_index_lower WHERE hasToken(lower(text), lower('Hello'));

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM test_text_index_lower WHERE hasToken(lower(text), lower('Hello'))
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

SELECT count() FROM test_text_index_lower WHERE searchAll(lower(text), [lower('Hello')]);

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM test_text_index_lower WHERE searchAll(lower(text), [lower('Hello')])
) WHERE explain LIKE '%Filter column%' OR explain LIKE '%Name: idx_text%';

DROP TABLE IF EXISTS test_text_index_lower;
