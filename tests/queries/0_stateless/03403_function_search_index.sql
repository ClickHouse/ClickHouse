set allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'default') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'Hello, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello, World' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouse is the fast, really fast!' FROM numbers(1024);

SELECT 'searchAny is used during index analysis';

SELECT 'Skip index should choose none for non-existent term';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Click');

SELECT 'Skip index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Hallo');

SELECT 'Skip index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Hallo Word'); -- Word does not exist in terms

SELECT 'Skip index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Hello');

SELECT 'Skip index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Hello World');

SELECT 'Skip index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Hallo World');

SELECT 'Skip index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'Hello Hallo');

SELECT 'Skip index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'ClickHouse');

SELECT 'Skip index should choose all 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAny(message, 'ClickHouse World');

SELECT 'searchAll is used during index analysis';

SELECT 'Skip index should choose none for non-existent term';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Click');

SELECT 'Skip index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Hallo');

SELECT 'Skip index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Hello World');

SELECT 'Skip index should choose none if any term does not exists in dictionary';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Hallo Word'); -- Word does not exist in terms

SELECT 'Skip index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Hello');

SELECT 'Skip index should choose none';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Hallo World');

SELECT 'Skip index should choose none';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'Hello Hallo');

SELECT 'Skip index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'ClickHouse');

SELECT 'Skip index should choose none';
EXPLAIN indexes=1
SELECT count() FROM tab WHERE searchAll(message, 'ClickHouse World');

DROP TABLE tab;
