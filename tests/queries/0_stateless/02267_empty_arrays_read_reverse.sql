DROP TABLE IF EXISTS t_02267;

CREATE TABLE t_02267
(
    a Array(String),
    b UInt32,
    c Array(String)
)
ENGINE = MergeTree
ORDER BY b
SETTINGS index_granularity = 500, index_granularity_bytes = '10Mi';

INSERT INTO t_02267 (b, a, c) SELECT 0, ['x'],  ['1','2','3','4','5','6'] FROM numbers(1) ;
INSERT INTO t_02267 (b, a, c) SELECT 1, [],     ['1','2','3','4','5','6'] FROM numbers(300000);

OPTIMIZE TABLE t_02267 FINAL;

SELECT * FROM t_02267 WHERE hasAll(a, ['x'])
ORDER BY b DESC
SETTINGS max_threads=1, max_block_size=1000;

DROP TABLE IF EXISTS t_02267;
