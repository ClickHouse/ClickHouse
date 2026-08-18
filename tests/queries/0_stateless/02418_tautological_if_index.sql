SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

DROP TABLE IF EXISTS constCondOptimization;

CREATE TABLE constCondOptimization
(
    d Date DEFAULT today(),
    time DateTime DEFAULT now(),
    n Int64
)
ENGINE = MergeTree ORDER BY (time, n) SETTINGS index_granularity = 1;

INSERT INTO constCondOptimization (n) SELECT number FROM system.numbers LIMIT 10000;

-- The queries should use index.
SET max_rows_to_read = 2;

SELECT count() FROM constCondOptimization WHERE if(0, 1, n = 1000);
SELECT count() FROM constCondOptimization WHERE if(0, 1, n = 1000) AND 1 = 1;

DROP TABLE constCondOptimization;
