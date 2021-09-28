CREATE TABLE a (number UInt64) ENGINE = MergeTree ORDER BY if(now() > toDateTime('2020-06-01 13:31:40'), toInt64(number), -number); -- { serverError 36 }
CREATE TABLE b (number UInt64) ENGINE = MergeTree ORDER BY now() > toDateTime(number); -- { serverError 36 }
CREATE TABLE c (number UInt64) ENGINE = MergeTree ORDER BY now(); -- { serverError 36 }
