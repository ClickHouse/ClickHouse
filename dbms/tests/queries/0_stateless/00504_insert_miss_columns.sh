#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# https://github.com/yandex/ClickHouse/issues/1300

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.advertiser";
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.advertiser_test";
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.advertiser ( action_date Date, adblock UInt8, imps Int64 ) Engine = SummingMergeTree( action_date, ( adblock ), 8192, ( imps ) )";
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.advertiser_test ( action_date Date, adblock UInt8, imps Int64, Hash UInt64 ) Engine = SummingMergeTree( action_date, ( adblock, Hash ), 8192, ( imps ) )";

# This test will fail. It's ok.
$CLICKHOUSE_CLIENT -q "INSERT INTO test.advertiser_test SELECT *, sipHash64( CAST(adblock  AS String) ), CAST(1 AS Int8) FROM test.advertiser;" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE test.advertiser";
$CLICKHOUSE_CLIENT -q "DROP TABLE test.advertiser_test";
$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'";
