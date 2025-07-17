DROP TABLE IF EXISTS bloom_filter_has_const_array;

CREATE TABLE bloom_filter_has_const_array
(
    `s` String,
    INDEX bf s TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY ()
SETTINGS index_granularity = 1;

INSERT INTO bloom_filter_has_const_array VALUES ('a'), ('b'), ('c'), ('d');

SET force_index_by_date = 0, force_primary_key = 0;

EXPLAIN indexes = 1
SELECT *
FROM bloom_filter_has_const_array
WHERE has(['a', 'c'], s);

SELECT *
FROM bloom_filter_has_const_array
WHERE has(['a', 'c'], s)
ORDER BY s;

DROP TABLE IF EXISTS bloom_filter_has_const_array;
