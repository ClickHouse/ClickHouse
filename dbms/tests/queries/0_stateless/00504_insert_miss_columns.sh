#!/usr/bin/env bash

# https://github.com/yandex/ClickHouse/issues/1300

clickhouse-client -q "DROP TABLE IF EXISTS test.advertiser";
clickhouse-client -q "DROP TABLE IF EXISTS test.advertiser_test";
clickhouse-client -q "CREATE TABLE test.advertiser ( action_date Date, adblock UInt8, imps Int64 ) Engine = SummingMergeTree( action_date, ( adblock ), 8192, ( imps ) )";
clickhouse-client -q "CREATE TABLE test.advertiser_test ( action_date Date, adblock UInt8, imps Int64, Hash UInt64 ) Engine = SummingMergeTree( action_date, ( adblock, Hash ), 8192, ( imps ) )";

# This test will fail. It's ok.
clickhouse-client -q "INSERT INTO test.advertiser_test SELECT *, sipHash64( CAST(adblock  AS String) ), CAST(1 AS Int8) FROM test.advertiser;" 2>/dev/null
clickhouse-client -q "DROP TABLE test.advertiser";
clickhouse-client -q "DROP TABLE test.advertiser_test";
clickhouse-client -q "SELECT 'server still alive'";
