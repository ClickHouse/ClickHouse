-- { echo ON }

SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `i` Int64,
    `s` String,
    INDEX bf_s s TYPE bloom_filter(0.001) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity = 4,index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT 100, 'aaa'; -- Single-granule part, which contained rows less than index_granularity.

SELECT i FROM tab WHERE s = 'aaa';
