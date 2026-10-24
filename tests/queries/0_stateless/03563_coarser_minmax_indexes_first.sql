DROP TABLE IF EXISTS skip_table;

CREATE TABLE skip_table
(
    k UInt64,
    v UInt64,
    INDEX mm_fine v TYPE minmax GRANULARITY 1, -- fine-grained mm-index first
    INDEX mm_coarse v TYPE minmax GRANULARITY 1024
)
ENGINE = MergeTree
PRIMARY KEY k
SETTINGS index_granularity = 8192;

INSERT INTO skip_table SELECT number, intDiv(number, 4096) FROM numbers(100000);

SELECT trim(explain) FROM ( EXPLAIN indexes = 1 SELECT * FROM skip_table WHERE v = 125 SETTINGS per_part_index_stats=1) WHERE explain like '%Name%';

DROP TABLE skip_table;
