DROP TABLE IF EXISTS t_sparse_grams;
SET enable_full_text_index = 1;

CREATE TABLE t_sparse_grams
(
    s String,
    INDEX idx_s(s) TYPE text(tokenizer = sparseGrams(3, 20, 5), preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_sparse_grams (s) VALUES ('Hello, world!');
INSERT INTO t_sparse_grams (s) VALUES ('ClickHouse is the fastest OLAP database');

SELECT sparseGrams(lower('the fastest OLAP database'), 3, 20, 5);
SELECT s FROM t_sparse_grams WHERE s LIKE '%the fastest OLAP database%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT s FROM t_sparse_grams WHERE s LIKE '%the fastest OLAP database%'
)
WHERE explain LIKE '%Condition:%';

DROP TABLE IF EXISTS t_sparse_grams;
