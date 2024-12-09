CREATE TABLE test_03096_sum
(
    id UInt64,
    col1 Nullable(UInt64),
    col2_null Nullable(UInt64),
    col3_null Nullable(UInt64),
    time UInt64
)
    ENGINE = AggregatingMergeTree(sum)
        ORDER BY (id, time);


INSERT INTO test_03096_sum VALUES (1,1,3,5,3), (1,0,8,NULL,3), (2,0,8,4,3), (1,0,8,4,4);
SELECT * FROM test_03096_sum FINAL FORMAT CSV;

DROP TABLE test_03096_sum SYNC;

CREATE TABLE test_03096_nonsimple
(
    `id` UInt64,
    `val` UInt64
)
    ENGINE = AggregatingMergeTree(avg)
        ORDER BY (id);  -- {serverError 36}
