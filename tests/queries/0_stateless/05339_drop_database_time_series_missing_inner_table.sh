#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DROP DATABASE` succeeds when a `TimeSeries` table (plain or the inner table of a materialized view) has lost one of its inner tables.

db="${CLICKHOUSE_DATABASE}_1"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db ENGINE = Atomic"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE $db.ts ENGINE = TimeSeries"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE MATERIALIZED VIEW $db.mv ENGINE = TimeSeries AS SELECT 1 AS a"

for t in $($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '$db' AND name LIKE '.inner_id.tags.%'"); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE $db.\`$t\` SYNC SETTINGS ignore_drop_queries_probability = 0"
done

$CLICKHOUSE_CLIENT -q "SELECT countIf(name LIKE '.inner_id.tags.%'), countIf(name IN ('ts', 'mv')), countIf(name LIKE '.inner_id.samples.%') FROM system.tables WHERE database = '$db'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $db SYNC"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"
