#!/usr/bin/env bash

# Tests profile event "SelectedMarksByPrimaryKeyUsage"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table_id="$(random_str 10)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS table_$table_id;"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE table_$table_id (
        pk Int64,
        col1 Int64,
        col2 Int64,
        INDEX idx(col2) TYPE minmax
    ) ENGINE = MergeTree ORDER BY pk PARTITION BY (pk % 2);";

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE table_$table_id ADD PROJECTION proj (SELECT * ORDER BY col1);"

# Populate two partitions with 50k rows each. Each partition has >1 granules.
# We want SelectQueriesWithPrimaryKeyUsage to increase by +1 in each query, not by +1 per partition or by +1 per granule.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO table_$table_id SELECT number, number, number FROM numbers(100000);"

# Run SELECTs

# -- No filter
query_id="$(random_str 10)"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT count(*) FROM table_$table_id FORMAT Null;"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT
        ProfileEvents['SelectQueriesWithPrimaryKeyUsage'] AS selects_with_pk_usage
    FROM
        system.query_log
    WHERE
        current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$query_id'
    FORMAT TSVWithNames;
"

# -- Filter on non-PK column. However, it has a minmax-index defined. We expect the profile event to not increase.
query_id="$(random_str 10)"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT count(*) FROM table_$table_id WHERE col2 >= 50000 FORMAT Null;"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT
        ProfileEvents['SelectQueriesWithPrimaryKeyUsage'] AS selects_with_pk_usage
    FROM
        system.query_log
    WHERE
        current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$query_id'
    FORMAT TSVWithNames;
"

# Filter on PK
query_id="$(random_str 10)"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT count(*) FROM table_$table_id WHERE pk >= 50000 FORMAT Null;"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT
        ProfileEvents['SelectQueriesWithPrimaryKeyUsage'] AS selects_with_pk_usage
    FROM
        system.query_log
    WHERE
        current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$query_id'
    FORMAT TSVWithNames;
"

# Filter on PK in projection
query_id="$(random_str 10)"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT count(*) FROM table_$table_id WHERE col1 >= 50000 FORMAT Null;"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT
        ProfileEvents['SelectQueriesWithPrimaryKeyUsage'] AS selects_with_pk_usage
    FROM
        system.query_log
    WHERE
        current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id = '$query_id'
    FORMAT TSVWithNames;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE table_$table_id;"
