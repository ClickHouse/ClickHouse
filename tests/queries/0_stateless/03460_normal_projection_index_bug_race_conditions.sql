-- { echo ON }

SET max_threads=8;
SET max_projection_rows_to_use_projection_index = 102400000;
SET min_table_rows_to_use_projection_index = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `i` Int64,
    `a` Int64,
    `text` String,
    PROJECTION by_text
    (
        SELECT _part_offset
        ORDER BY text
    )
)
ENGINE = MergeTree
PARTITION BY i % 4
ORDER BY a
SETTINGS index_granularity = 8192;

INSERT INTO tab SELECT number, number, number FROM numbers(8192 * 10);

SELECT * FROM tab WHERE text = '1000' SETTINGS use_query_condition_cache = 0, optimize_use_projections = 1, optimize_use_projection_filtering = 1;
