DROP TABLE IF EXISTS skip_table;

CREATE TABLE skip_table
(
    k UInt64,
    v UInt64,
    INDEX v_set v TYPE set(100) GRANULARITY 2, -- set index is declared before minmax intentionally
    INDEX v_mm v TYPE minmax GRANULARITY 2
)
ENGINE = MergeTree
PRIMARY KEY k
SETTINGS index_granularity = 8192;

INSERT INTO skip_table SELECT number, intDiv(number, 4096) FROM numbers(100000);
SELECT trim(explain) FROM ( EXPLAIN indexes = 1 SELECT * FROM skip_table WHERE v = 125 SETTINGS per_part_index_stats=1) WHERE explain like '%Name%';

DROP TABLE skip_table;
