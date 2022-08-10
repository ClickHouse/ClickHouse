#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We will check that --time option of clickhouse-client will display server-side time, not client-side time.
# For this purpose, we ask it to output some amount of data that is larger than the buffers in clickhouse-client
# but smaller than the pipe buffer, and we slow down output of the query result with 'pv' command to get around one second of run time.
# If everything is works as expected, at least sometimes this command should display time less than 100ms.
while true
do
    $CLICKHOUSE_CLIENT --time --query 'SELECT 1 FROM numbers(1000000) FORMAT RowBinary' 2>"${CLICKHOUSE_TMP}/time" | pv --quiet --rate-limit 1M > /dev/null
    grep -q -F '0.0' "${CLICKHOUSE_TMP}/time" && break
done

echo 'Ok'
