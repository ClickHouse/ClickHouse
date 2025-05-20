#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query="SELECT number FROM system.numbers INTO OUTFILE test.native.zst FORMAT Native" 2>&1 | grep -q "Code: 62. DB::Exception: Syntax error: failed at position 48 ('test'): test.native.zst FORMAT Native. Expected string literal." && echo 'OK' || echo 'FAIL' ||:

