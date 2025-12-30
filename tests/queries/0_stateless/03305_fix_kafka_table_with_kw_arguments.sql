-- Tags: no-fasttest

CREATE TABLE default.test
(
    `id` UInt32,
    `message` String
)
ENGINE = Kafka(a = '1', 'clickhouse'); -- { serverError BAD_ARGUMENTS }
