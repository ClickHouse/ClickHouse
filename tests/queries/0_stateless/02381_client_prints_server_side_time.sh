#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-object-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We will check that --time option of clickhouse-client will display server-side time, not client-side time.
# For this purpose, we ask it to output some amount of data that is larger than the buffers in clickhouse-client
# but smaller than the pipe buffer, and we slow down output of the query result with 'pv' command to more than three
# seconds of run time, so a time under two seconds can only be the server-side one. A loaded server, as in the flaky
# check where several copies of this test run at once, often takes more than 100 ms.
while clickhouse_test_time_left
do
    $CLICKHOUSE_CLIENT --time --query 'SELECT 1 FROM numbers(1000000) FORMAT RowBinary' 2>"${CLICKHOUSE_TMP}/time" | pv --quiet --rate-limit 300K > /dev/null
    awk '/^[0-9.]+$/ && $1 < 2 {found = 1} END {exit !found}' "${CLICKHOUSE_TMP}/time" && break
done

echo 'Ok'
