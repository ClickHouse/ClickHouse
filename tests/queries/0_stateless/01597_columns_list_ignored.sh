#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "describe table file('', TSV, 'a int, b.c int')" 2>&1 | grep -F -c 'Syntax error'
