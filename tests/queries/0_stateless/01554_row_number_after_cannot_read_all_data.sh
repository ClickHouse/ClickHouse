#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo -n -e '\x01\x00\x00\x00\x05Hello\x80' | ${CLICKHOUSE_LOCAL} --structure 'x UInt32, s String' --query "SELECT * FROM table" --input-format RowBinary 2>&1 | grep -oF '(at row 2)'
