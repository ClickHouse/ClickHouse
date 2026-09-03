-- Tsts that covered sparse grams are filtered out.

DROP TABLE IF EXISTS tab;

-- To have always local plan in EXPLAIN when running the test with enabled parallel replicas
SET parallel_replicas_local_plan = 1;

CREATE TABLE tab
(
    s String,
    INDEX idx_s(s) TYPE text(tokenizer = sparseGrams(3, 20, 5), preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab (s) VALUES ('Hello, world!');
INSERT INTO tab (s) VALUES ('ClickHouse is the fastest OLAP database');

SELECT sparseGrams(lower('the fastest OLAP database'), 3, 20, 5);
SELECT s FROM tab WHERE s LIKE '%the fastest OLAP database%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT s FROM tab WHERE s LIKE '%the fastest OLAP database%'
)
WHERE explain LIKE '%Condition:%';

DROP TABLE tab;
