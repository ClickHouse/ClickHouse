#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


PORT="$(($RANDOM%63000+2001))"

TEMP_FILE="${CLICKHOUSE_TMP}/01622_defaults_for_url_engine.tmp"

function thread1
{
    while true; do
        echo -e "HTTP/1.1 200 OK\n\n{\"a\": 1}" | nc -l -p $1 -q 1;
    done
}

function thread2
{
    while true; do
        $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 -q "SELECT * FROM url('http://127.0.0.1:$1/', JSONEachRow, 'a int, b int default 7, c default a + b') format Values"
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

TIMEOUT=5

timeout $TIMEOUT bash -c "thread1 $PORT" > /dev/null 2>&1 &
timeout $TIMEOUT bash -c "thread2 $PORT" 2> /dev/null > $TEMP_FILE &

wait

grep -q '(1,7,8)' $TEMP_FILE && echo "Ok"
