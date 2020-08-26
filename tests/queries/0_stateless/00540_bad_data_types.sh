#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS Array)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS AggregateFunction)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS Nullable)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS Tuple)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS FixedString)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS Enum)" 2>/dev/null || true;
$CLICKHOUSE_CLIENT --query="SELECT CAST(0 AS DateTime)";

echo

($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE toString(dummy)" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE toString(1)" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE 256" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE -1" 2>/dev/null && echo "Expected failure") || true;
($CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE CAST(256 AS Nullable(UInt16))" 2>/dev/null && echo "Expected failure") || true;

$CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE CAST(NULL AS Nullable(UInt8))"
$CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE 255"
$CLICKHOUSE_CLIENT --query="SELECT * FROM system.one WHERE CAST(255 AS Nullable(UInt8))"
