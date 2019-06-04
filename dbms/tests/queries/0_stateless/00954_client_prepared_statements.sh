#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ps";
$CLICKHOUSE_CLIENT -q "CREATE TABLE ps (i UInt8, s String, d DateTime) ENGINE = Memory";

$CLICKHOUSE_CLIENT -q "INSERT INTO ps VALUES (1, 'Hello, world', '2005-05-05 05:05:05')";
$CLICKHOUSE_CLIENT -q "INSERT INTO ps VALUES (2, 'test', '2005-05-25 15:00:00')";

$CLICKHOUSE_CLIENT --max_threads=1 --param_id=1\
    -q "SELECT * FROM ps WHERE i = {id:UInt8}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_phrase='Hello, world'\
    -q "SELECT * FROM ps WHERE s = {phrase:String}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_date='2005-05-25 15:00:00'\
    -q "SELECT * FROM ps WHERE d = {date:DateTime}";
$CLICKHOUSE_CLIENT --max_threads=1 --param_id=2 --param_phrase='test'\
    -q "SELECT * FROM ps WHERE i = {id:UInt8} and s = {phrase:String}";

$CLICKHOUSE_CLIENT -q "DROP TABLE ps";
