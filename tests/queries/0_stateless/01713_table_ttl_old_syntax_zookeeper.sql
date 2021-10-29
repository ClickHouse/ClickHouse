-- Tags: zookeeper

DROP TABLE IF EXISTS ttl_table;

CREATE TABLE ttl_table
(
    date Date,
    value UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
TTL date + INTERVAL 2 MONTH; --{ serverError 36 }

CREATE TABLE ttl_table
(
    date Date,
    value UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
PARTITION BY date; --{ serverError 42 }

CREATE TABLE ttl_table
(
    date Date,
    value UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
ORDER BY value; --{ serverError 42 }

SELECT 1;

DROP TABLE IF EXISTS ttl_table;
