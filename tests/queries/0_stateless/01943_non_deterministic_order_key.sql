CREATE TABLE a (number UInt64) ENGINE = MergeTree ORDER BY if(now() > toDateTime('2020-06-01 13:31:40'), toInt64(number), -number); -- { serverError BAD_ARGUMENTS }
CREATE TABLE b (number UInt64) ENGINE = MergeTree ORDER BY now() > toDateTime(number); -- { serverError BAD_ARGUMENTS }
CREATE TABLE c (number UInt64) ENGINE = MergeTree ORDER BY now(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE d (number UInt64) ENGINE = MergeTree ORDER BY now() + 1 + 1 + number; -- { serverError BAD_ARGUMENTS }
