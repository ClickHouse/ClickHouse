drop table if exists test;
CREATE TABLE test (
    `start_s`  UInt32 EPHEMERAL COMMENT 'start UNIX time' ,
    `start_us` UInt16 EPHEMERAL COMMENT 'start microseconds',
    `finish_s`  UInt32 EPHEMERAL COMMENT 'finish UNIX time',
    `finish_us` UInt16 EPHEMERAL COMMENT 'finish microseconds',
    `captured` DateTime MATERIALIZED fromUnixTimestamp(start_s),
    `duration` Decimal32(6) MATERIALIZED finish_s - start_s + (finish_us - start_us)/1000000
)
ENGINE Null;
drop table if exists test;
