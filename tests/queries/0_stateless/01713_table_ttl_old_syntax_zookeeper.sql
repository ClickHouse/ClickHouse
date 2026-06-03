-- Tags: zookeeper

DROP TABLE IF EXISTS ttl_table;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE ttl_table
(
    date Date,
    value UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
TTL date + INTERVAL 2 MONTH; --{ serverError BAD_ARGUMENTS }

CREATE TABLE ttl_table
(
    date Date,
    value UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
PARTITION BY date; --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

CREATE TABLE ttl_table
(
    date Date,
    value UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
ORDER BY value; --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 1;

DROP TABLE IF EXISTS ttl_table;
