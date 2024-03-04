DROP TABLE IF EXISTS skip_table;

CREATE TABLE skip_table
(
    k UInt64,
    v UInt64,
    INDEX v_set v TYPE set(100) GRANULARITY 2, -- set index is declared before minmax intentionally
    INDEX v_mm v TYPE minmax GRANULARITY 2
)
ENGINE = MergeTree
PRIMARY KEY k;

INSERT INTO skip_table SELECT number, intDiv(number, 4096) FROM numbers(1000000);

SELECT trim(explain) FROM ( EXPLAIN indexes = 1 SELECT * FROM skip_table WHERE v = 125) WHERE explain like '%Name%';
