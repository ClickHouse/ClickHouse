#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.ws";
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.ws (i UInt8) ENGINE = Memory";

$CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT RowBinary ;";
$CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT RowBinary 	; ";
$CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT RowBinary
; ";
echo -n ";" | $CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT RowBinary";

$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM test.ws";
$CLICKHOUSE_CLIENT -q "DROP TABLE test.ws";


$CLICKHOUSE_CLIENT -q "SELECT ''";


$CLICKHOUSE_CLIENT -q "CREATE TABLE test.ws (s String) ENGINE = Memory";
$CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT TSV	;
";
echo ";" | $CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT TSV"
if $CLICKHOUSE_CLIENT -q "INSERT INTO test.ws FORMAT TSV;" 1>/dev/null 2>/dev/null; then
    echo ERROR;
fi
$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT * FROM test.ws";

$CLICKHOUSE_CLIENT -q "DROP TABLE test.ws";
