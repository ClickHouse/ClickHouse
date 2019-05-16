#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

echo "SELECT 1 IN 1" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN (1)" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN (1, 2)" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN f(1)" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN (f(1))" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN (f(1), f(2))" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN f(1, 2)" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN 1 + 1" | $CLICKHOUSE_FORMAT # This is quite strange
echo "SELECT 1 IN 'hello'" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN f('hello')" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN ('hello', 'world')" | $CLICKHOUSE_FORMAT
echo "SELECT 1 IN f('hello', 'world')" | $CLICKHOUSE_FORMAT
