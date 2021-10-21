#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch test_exception
$CLICKHOUSE_LOCAL --query="SELECT 1 INTO OUTFILE 'test_exception' FORMAT Native" 2>&1 | grep -q "Code: 76. DB::ErrnoException:" && echo 'OK' || echo 'FAIL' ||:
