-- Tags: no-s3-storage
DROP TABLE IF EXISTS t;

CREATE TABLE t (`key` UInt32, `created_at` Date, `value` UInt32, PROJECTION xxx (SELECT key, created_at, sum(value) GROUP BY key, created_at)) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) ORDER BY key;

INSERT INTO t SELECT 1 AS key, today() + (number % 30), number FROM numbers(1000);

ALTER TABLE t UPDATE value = 0 WHERE (value > 0) AND (created_at >= '2021-12-21') SETTINGS allow_experimental_projection_optimization = 1;

DROP TABLE IF EXISTS t;
