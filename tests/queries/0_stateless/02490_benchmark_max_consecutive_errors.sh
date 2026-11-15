#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK -q 'select throwIf(1)' |& grep "DB::Exception: Value passed to 'throwIf' function is non-zero" -c
RES=$($CLICKHOUSE_BENCHMARK --max-consecutive-errors 10 -q 'select throwIf(1)' |& tee "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.log" | grep "DB::Exception: Value passed to 'throwIf' function is non-zero" -c)

if [ "$RES" -eq 10 ]
then
    echo "$RES"
else
    echo "$RES"
    cat "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.log"
fi
