#!/usr/bin/env bash
# Tags: no-ordinary-database, no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3"

# there are some issues with Atomic database, let's generate it uniq
# otherwise flaky check will not pass.
uuid=$(${CLICKHOUSE_CLIENT} --query "SELECT reinterpretAsUUID(currentDatabase())")

echo "DROP TABLE IF EXISTS tab_00738 SYNC;
DROP TABLE IF EXISTS mv SYNC;
CREATE TABLE tab_00738(a Int) ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
-- The matview will take at least 2 seconds to be finished (10000000 * 0.0000002)
CREATE MATERIALIZED VIEW mv UUID '$uuid' ENGINE = Log AS SELECT sleepEachRow(0.0000002) FROM tab_00738;" | ${CLICKHOUSE_CLIENT} -n

${CLICKHOUSE_CLIENT} --query_id insert_$CLICKHOUSE_DATABASE --query "INSERT INTO tab_00738 SELECT number FROM numbers(10000000)" &

function drop_inner_id()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE \`.inner_id.$uuid\`" -n
}

function wait_for_query_to_start()
{
    while [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = 'insert_$CLICKHOUSE_DATABASE'") == 0 ]]; do sleep 0.001; done

    # The query is already started, but there is no guarantee that it locks the underlying table already.
    # Wait until PushingToViews chain will acquire the lock of the underlying table for the INSERT query.
    # (assume that 0.5 second is enough for this, but this is not 100% correct)
    sleep 0.5

    # query already finished, fail
    if [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = 'insert_$CLICKHOUSE_DATABASE'") == 0 ]]; then
        return 2
    fi
}

function drop_at_exit()
{
    echo "DROP TABLE IF EXISTS tab_00738;
DROP TABLE IF EXISTS mv;" | ${CLICKHOUSE_CLIENT} -n
}

ret_code=0
export -f wait_for_query_to_start
timeout 15 bash -c wait_for_query_to_start || ret_code=$?

if [[ $ret_code ==  124 ]] || [[ $ret_code ==  2 ]]; then
    # suppressing test inaccuracy
    # $ret_code == 124 -- wait_for_query_to_start didn't catch the insert command in running state
    # $ret_code == 2 -- wait_for_query_to_start caught the insert command running but command ended too fast
    wait
    drop_at_exit
    exit 0
fi

drop_inner_id

wait

drop_at_exit
