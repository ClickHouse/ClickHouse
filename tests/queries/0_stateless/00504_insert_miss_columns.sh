#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/1300

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS advertiser";
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS advertiser_test";
$CLICKHOUSE_CLIENT -q "CREATE TABLE advertiser ( action_date Date, adblock UInt8, imps Int64 ) Engine = SummingMergeTree( action_date, ( adblock ), 8192, ( imps ) )";
$CLICKHOUSE_CLIENT -q "CREATE TABLE advertiser_test ( action_date Date, adblock UInt8, imps Int64, Hash UInt64 ) Engine = SummingMergeTree( action_date, ( adblock, Hash ), 8192, ( imps ) )";

# This test will fail. It's ok.
$CLICKHOUSE_CLIENT -q "INSERT INTO advertiser_test SELECT *, sipHash64( CAST(adblock  AS String) ), CAST(1 AS Int8) FROM advertiser;" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE advertiser";
$CLICKHOUSE_CLIENT -q "DROP TABLE advertiser_test";
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'";
