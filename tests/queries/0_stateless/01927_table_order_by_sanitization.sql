CREATE TABLE a
(
    `number` UInt64
)
    ENGINE = MergeTree
ORDER BY if(now() > toDateTime('2020-06-01 13:31:40'), toInt64(number), -number); -- { serverError 36 }

CREATE TABLE a
(
    `number` UInt64
)
    ENGINE = MergeTree
ORDER BY if(rand() > 100, toInt64(number), -number);  -- { serverError 36 }