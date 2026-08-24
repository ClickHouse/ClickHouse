SET group_by_two_level_threshold_bytes = 1;
SET group_by_two_level_threshold = 1;

SELECT groupArray(DISTINCT toString(number % 10)) FROM numbers_mt(50000) 
    GROUP BY number ORDER BY number LIMIT 10
    SETTINGS max_threads = 2, max_block_size = 2000;

DROP TABLE IF EXISTS distinct_two_level;

CREATE TABLE distinct_two_level (
    time DateTime64(3),
    domain String,
    subdomain String
) ENGINE = MergeTree ORDER BY time;

INSERT INTO distinct_two_level SELECT 1546300800000, 'test.com', concat('foo', toString(number % 10000)) from numbers(10000);
INSERT INTO distinct_two_level SELECT 1546300800000, concat('test.com', toString(number / 10000)) , concat('foo', toString(number % 10000)) from numbers(10000);

SELECT
    domain, arrayUniq(groupArraySample(5, 11111)(DISTINCT subdomain)) AS example_subdomains
FROM distinct_two_level
GROUP BY domain ORDER BY domain, example_subdomains
LIMIT 10;

DROP TABLE IF EXISTS distinct_two_level;
