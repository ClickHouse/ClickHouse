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
