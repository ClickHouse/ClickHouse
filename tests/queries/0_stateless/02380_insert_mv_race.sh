#!/usr/bin/env bash
# Tags: long, race

# Regression test for INSERT into table with MV attached,
# to avoid possible errors if some table will disappears,
# in case of multiple streams was used (i.e. max_insert_threads>1)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function bootstrap()
{
    $CLICKHOUSE_CLIENT -nm -q "
        DROP TABLE IF EXISTS null;
        CREATE TABLE null (key Int) ENGINE = Null;

        DROP TABLE IF EXISTS mv;
        CREATE MATERIALIZED VIEW mv ENGINE = Null() AS SELECT * FROM null;
    "
}

function insert_thread()
{
    local opts=(
        --max_insert_threads 100
        --max_threads 100
    )
    local patterns=(
        -e UNKNOWN_TABLE
        -e TABLE_IS_DROPPED
    )

    while :; do
        $CLICKHOUSE_CLIENT "${opts[@]}" -q "INSERT INTO null SELECT * FROM numbers_mt(1e6)" |& {
            grep -F "DB::Exception: " | grep -v -F "${patterns[@]}"
        }
    done
}
export -f insert_thread

function drop_thread()
{
    local opts=(
        --database_atomic_wait_for_drop_and_detach_synchronously 1
    )

    while :; do
        $CLICKHOUSE_CLIENT -nm "${opts[@]}" -q "DETACH TABLE mv"
        sleep 0.01
        $CLICKHOUSE_CLIENT -nm "${opts[@]}" -q "ATTACH TABLE mv"
    done
}
export -f drop_thread

function main()
{
    local test_timeout=1m

    bootstrap
    timeout "$test_timeout" bash -c insert_thread &
    timeout "$test_timeout" bash -c drop_thread &

    wait
}
main "$@"
