DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    v1 Int32,
    v2 Int32,
    INDEX v1idx v1 TYPE minmax
) Engine=MergeTree ORDER BY id SETTINGS index_granularity=64;

INSERT INTO tab SELECT number, number, number FROM numbers(10000);

-- Only 10 granules should be read (10th granule may have more than 64 rows)
SELECT id,v1 FROM tab ORDER BY v1 ASC LIMIT 10 SETTINGS max_rows_to_read=640, use_skip_indexes_for_top_n=1, use_skip_indexes_on_data_read=0;
SELECT id,v1 FROM tab ORDER BY v1 DESC LIMIT 10 SETTINGS max_rows_to_read=714, use_skip_indexes_for_top_n=1, use_skip_indexes_on_data_read=0;

-- will error out because optimization not possible due to WHERE clause
SELECT id,v1 FROM tab WHERE v2 > 0 ORDER BY v1 ASC LIMIT 10 SETTINGS max_rows_to_read=640, use_skip_indexes_for_top_n=1, use_skip_indexes_on_data_read=0;  -- { serverError TOO_MANY_ROWS }
