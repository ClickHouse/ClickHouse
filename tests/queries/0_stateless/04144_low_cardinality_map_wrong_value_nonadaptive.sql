SET low_cardinality_max_dictionary_size = 8192;
SET low_cardinality_use_single_dictionary_for_part = 0;

DROP TABLE IF EXISTS t_04063;

CREATE TABLE t_04063
(
    id UInt64,
    filter UInt8,
    s LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    index_granularity_bytes = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;

-- Non-adaptive wide parts have no final mark.
-- With low_cardinality_max_dictionary_size = 8192 and
-- low_cardinality_use_single_dictionary_for_part = 0,
-- the first 8192 unique values fill the first dictionary, which is written into
-- the dictionary stream before row 8193 starts. The remaining values go into a
-- second dictionary written in the suffix. Because the part is non-adaptive and
-- has no final mark, the dictionary stream still exposes only two distinct mark
-- positions for the whole part.
--
-- The read must also be discontinuous for the LC stream.
-- PREWHERE filter = 1 keeps the task on one contiguous range, but the main
-- reader skips whole middle granules. That makes the LC stream read row 1,
-- then restart on row 10000 inside the same reader state.
INSERT INTO t_04063
SELECT
    number + 1 AS id,
    if(number IN (0, 9999), 1, 0) AS filter,
    'value_' || toString(number + 1) AS s
FROM numbers(10000);

SELECT countDistinct(`s.dict.mark`)
FROM mergeTreeIndex(currentDatabase(), t_04063, with_marks = true)
WHERE part_name = (
    SELECT name
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_04063' AND active
    ORDER BY name DESC
    LIMIT 1
);

SELECT
    id,
    s
FROM t_04063
PREWHERE filter = 1
ORDER BY id
SETTINGS
    max_threads = 1,
    max_block_size = 1000000,
    optimize_read_in_order = 0;

DROP TABLE t_04063;
