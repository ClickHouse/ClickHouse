DROP TABLE IF EXISTS index_compact;

CREATE TABLE index_compact(a UInt32, b UInt32, index i1 b type minmax granularity 1)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1000, index_granularity = 128, merge_max_block_size = 100;

INSERT INTO index_compact SELECT number, toString(number) FROM numbers(100);
INSERT INTO index_compact SELECT number, toString(number) FROM numbers(30);

OPTIMIZE TABLE index_compact FINAL;

SELECT count() FROM index_compact WHERE b < 10;

DROP TABLE index_compact;
