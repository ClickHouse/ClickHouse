#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BINARY client --send_logs_level="trace" --query="SELECT 1" 2>&1 | awk '{ print $8 }' | grep "Trace" | head -n 1
echo "."
$CLICKHOUSE_BINARY client --send_logs_level="debug" --query="SELECT 1" 2>&1 | awk '{ print $8 }' | grep "Debug" | head -n 1
echo "."
$CLICKHOUSE_BINARY client --send_logs_level="information" --query="SELECT 1" 2>&1 | awk '{ print $8 }' | grep "Information" | head -n 1
echo "."
$CLICKHOUSE_BINARY client --send_logs_level="error" --query="SELECT throwIf(1)" 2>&1 | awk '{ print $8 }' | grep "Error" | head -n 1
echo "-"
$CLICKHOUSE_BINARY client --send_logs_level="debug" --query="SELECT 1" 2>&1 | awk '{ print $8 }' | grep "Trace" | head -n 1
echo "."
$CLICKHOUSE_BINARY client --send_logs_level="information" --query="SELECT 1" 2>&1 | awk '{ print $8 }' | grep "Debug\|Trace" | head -n 1
echo "."
$CLICKHOUSE_BINARY client --send_logs_level="error" --query="SELECT throwIf(1)" 2>&1 | awk '{ print $8 }' | grep "Debug\|Trace\|Information" | head -n 1
echo "."
$CLICKHOUSE_BINARY client --send_logs_level="None" --query="SELECT throwIf(1)" 2>&1 | awk '{ print $8 }' | grep "Debug\|Trace\|Information\|Error" | head -n 1
