#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch "${CLICKHOUSE_TMP}/test_exception"
function cleanup()
{
    rm "${CLICKHOUSE_TMP}/test_exception"
}
trap cleanup EXIT
$CLICKHOUSE_LOCAL --query="SELECT 1 INTO OUTFILE '${CLICKHOUSE_TMP}/test_exception' FORMAT Native" 2>&1 | grep -q "Code: 76. DB::ErrnoException:" && echo 'OK' || echo 'FAIL' ||:
