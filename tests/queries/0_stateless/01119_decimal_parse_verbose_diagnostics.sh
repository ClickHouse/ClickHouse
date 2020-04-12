#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT * FROM table" --input-format CSV --structure 'd Decimal64(10)' <<<'12345678'$'\n''123456789' 2>&1 | grep -v -F 'version' | sed 's/Exception//';
