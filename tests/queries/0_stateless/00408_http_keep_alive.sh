#!/usr/bin/env bash
# Tags: no-parallel

# no-parallel because of the `FLUSH ASYNC INSERT QUEUE` command

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

# the sed command here replaces the real number of left requests with a question mark, because it can vary and we don't really have control over it
${CLICKHOUSE_CURL} -vsS "${URL}" --data-binary @- <<< "SELECT 1" 2>&1 | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -i 'keep-alive';
${CLICKHOUSE_CURL} -vsS "${URL}" --data-binary @- <<< " error here " 2>&1 | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -i 'keep-alive';
${CLICKHOUSE_CURL} -vsS "${URL}"ping  2>&1 | perl -lnE 'print if /Keep-Alive/' | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -i 'keep-alive';

# no keep-alive:
${CLICKHOUSE_CURL} -vsS "${URL}"404/not/found/ 2>&1 | perl -lnE 'print if /Keep-Alive/';

# async inserts
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

tmp_prefix=00408_${CLICKHOUSE_DATABASE}
clean() {
    rm -f "$tmp_prefix"*
}
trap clean EXIT

for wait_for_async_insert in 0 1
do
    file_1="$(mktemp "$CURDIR/${tmp_prefix}_file_1.tmp.XXXXXX")"
    file_2="$(mktemp "$CURDIR/${tmp_prefix}_file_2.tmp.XXXXXX")"
    file_3="$(mktemp "$CURDIR/${tmp_prefix}_file_3.tmp.XXXXXX")"

    echo "wait_for_async_insert=$wait_for_async_insert"

    settings="wait_for_async_insert=$wait_for_async_insert&async_insert=1&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_ms=600000"
    # async_insert_max_query_number does not work without async_insert_deduplicate=1
    settings="${settings}&async_insert_max_query_number=3&async_insert_deduplicate=1"

    ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&${settings}&query_id=00408-wait-$wait_for_async_insert-$file_1" -d "INSERT INTO async_inserts FORMAT CSV
    1,\"a-$wait_for_async_insert\"
    2,\"b-$wait_for_async_insert\"" |& grep 'Connection:' > $file_1 &

    ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&${settings}&query_id=00408-wait-$wait_for_async_insert-$file_2" -d "INSERT INTO async_inserts FORMAT CSV
    qqqqqqqqqqq-$wait_for_async_insert" |& grep 'Connection:' > $file_2  &

    ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&${settings}&query_id=00408-wait-$wait_for_async_insert-$file_3" -d "INSERT INTO async_inserts FORMAT CSV
    4,\"c-$wait_for_async_insert\"
    3,\"d-$wait_for_async_insert\"" |& grep 'Connection:' > $file_3 &

    wait

    echo "good async insert"
    cat $file_1
    echo "bad async insert"
    cat $file_2
    echo "good async insert"
    cat $file_3

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE;"
done
