#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function random_str()
{
    local n=$1 && shift
    tr -cd '[:lower:]' < /dev/urandom | head -c"$n"
}
function test_query_duration_ms()
{
    local query_id
    query_id="01548_query_log_query_execution_ms-$SECONDS-$(random_str 6)"
    local query_opts=(
        "--log_query_threads=1"
        "--log_queries_min_type=QUERY_FINISH"
        "--log_queries=1"
        "--query_id=$query_id"
        "--format=Null"
    )
    $CLICKHOUSE_CLIENT "${query_opts[@]}" -q "select sleep(0.4)" || exit 1
    $CLICKHOUSE_CLIENT -q "system flush logs" || exit 1

    $CLICKHOUSE_CLIENT -q "
        select count()
        from system.query_log
        where
            query_id = '$query_id'
            and current_database = currentDatabase()
            and query_duration_ms between 400 and 800
            and event_date >= yesterday()
            and event_time >= now() - interval 1 minute;
    " || exit 1

    $CLICKHOUSE_CLIENT -q "
        -- at least two threads for processing
        -- (but one just waits for another, sigh)
        select count() == 2
        from system.query_thread_log
        where
            query_id = '$query_id'
            and current_database = currentDatabase()
            and query_duration_ms between 400 and 800
            and event_date >= yesterday()
            and event_time >= now() - interval 1 minute;
    " || exit 1
}

function main()
{
    # retries, since there is no guarantee that every time query will take ~0.4 second.
    local retries=20 i=0
    while [ "$(test_query_duration_ms | xargs)" != '1 1' ] && [[ $i < $retries ]]; do
        ((++i))
    done
}
main "$@"
