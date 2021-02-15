#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT --oneline"

echo "SELECT 1 IN 1" | $format
echo "SELECT 1 IN (1)" | $format
echo "SELECT 1 IN (1, 2)" | $format
echo "SELECT 1 IN f(1)" | $format
echo "SELECT 1 IN (f(1))" | $format
echo "SELECT 1 IN (f(1), f(2))" | $format
echo "SELECT 1 IN f(1, 2)" | $format
echo "SELECT 1 IN 1 + 1" | $format
echo "SELECT 1 IN 'hello'" | $format
echo "SELECT 1 IN f('hello')" | $format
echo "SELECT 1 IN ('hello', 'world')" | $format
echo "SELECT 1 IN f('hello', 'world')" | $format
echo "SELECT 1 IN (SELECT 1)" | $format
