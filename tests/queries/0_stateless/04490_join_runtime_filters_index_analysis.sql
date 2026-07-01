DROP TABLE IF EXISTS sales1;
DROP TABLE IF EXISTS sales2;

CREATE TABLE sales1
(
    id UInt64,
    country String,
    amount UInt64,
    INDEX idx_country country TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;

CREATE TABLE sales2
(
    id UInt64,
    country String,
    region String
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO sales1 SELECT number, ['US', 'IN', 'JP', 'ZM'][number % 4 + 1], number * 10 FROM numbers(200);
INSERT INTO sales2 SELECT number, ['US', 'IN', 'JP', 'ZM'][number % 4 + 1], ['AMER', 'APAC', 'EMEA'][number % 3 + 1] FROM numbers(200);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;

-- PK join
SELECT s1.id, s1.country, s1.amount
FROM sales1 AS s1
INNER JOIN sales2 AS s2 ON s1.id = s2.id
WHERE s2.region = 'APAC'
ORDER BY s1.id;

-- skip index column in join
SELECT s1.country, count(), sum(s1.amount)
FROM sales1 AS s1
INNER JOIN sales2 AS s2 ON s1.country = s2.country
WHERE s2.region = 'EMEA'
GROUP BY s1.country
ORDER BY s1.country;

DROP TABLE sales1;
DROP TABLE sales2;
