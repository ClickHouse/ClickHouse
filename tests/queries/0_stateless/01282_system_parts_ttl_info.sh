#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./merges.lib
. "$CURDIR"/merges.lib
set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ttl (d DateTime) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 10 DAY SETTINGS remove_empty_parts=0;"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES ttl;"
$CLICKHOUSE_CLIENT --query "INSERT INTO ttl VALUES ('2000-01-01 01:02:03'), ('2000-02-03 04:05:06');"
$CLICKHOUSE_CLIENT --query "SELECT rows, delete_ttl_info_min, delete_ttl_info_max, move_ttl_info.expression, move_ttl_info.min, move_ttl_info.max FROM system.parts WHERE database = currentDatabase() AND table = 'ttl';"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES ttl;"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ttl FINAL;"
wait_for_merges_done ttl
$CLICKHOUSE_CLIENT --query "SELECT rows, toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC'), move_ttl_info.expression, move_ttl_info.min, move_ttl_info.max FROM system.parts WHERE database = currentDatabase() AND table = 'ttl' AND active;"
$CLICKHOUSE_CLIENT --query "DROP TABLE ttl;"
