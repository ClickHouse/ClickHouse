#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/836
# Inserting into a table that a materialized view references inside a column-renaming
# subquery on one side of a JOIN used to fail with a confusing
# `Not found column low_un in block` error. The insert must succeed and the view
# must receive the correctly joined rows.
# The analyzer is not randomized by the test harness, so run the scenario explicitly
# under both the analyzer (enable_analyzer=1) and the old analyzer (enable_analyzer=0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for enable_analyzer in 0 1
do
    $CLICKHOUSE_CLIENT --enable_analyzer "$enable_analyzer" --query "
        DROP TABLE IF EXISTS twitter_matching;
        DROP TABLE IF EXISTS twitter1_raw;
        DROP TABLE IF EXISTS twitter2_raw;
        DROP TABLE IF EXISTS twitter;

        CREATE TABLE twitter_matching (channel_id String, username String) ENGINE = Memory;
        CREATE TABLE twitter1_raw (username String, date Date, retweets UInt32) ENGINE = Memory;
        CREATE TABLE twitter2_raw (username String, date Date, retweets UInt32) ENGINE = Memory;

        CREATE MATERIALIZED VIEW twitter ENGINE = MergeTree ORDER BY (channel_id, date) AS
        SELECT channel_id, date, username, retweets
        FROM (SELECT lower(username) AS low_un, channel_id FROM twitter_matching) AS matching
        ALL INNER JOIN
        (
            SELECT lower(username) AS low_un, * FROM twitter1_raw
            UNION ALL
            SELECT lower(username) AS low_un, * FROM twitter2_raw
        ) AS tw USING low_un;

        INSERT INTO twitter1_raw VALUES ('Alice', '2023-01-01', 1);
        INSERT INTO twitter2_raw VALUES ('bob', '2023-02-02', 7);

        -- This insert triggers the materialized view; it used to throw.
        INSERT INTO twitter_matching VALUES ('chan1', 'ALICE'), ('chan2', 'Bob');

        SELECT channel_id, username, date, retweets FROM twitter ORDER BY channel_id;

        DROP TABLE twitter;
        DROP TABLE twitter_matching;
        DROP TABLE twitter1_raw;
        DROP TABLE twitter2_raw;
    "
done
