#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# there are some issues with Atomic database, let's generate it uniq
# otherwise flaky check will not pass.
uuid=$(${CLICKHOUSE_CLIENT} --query "SELECT reinterpretAsUUID(currentDatabase())")

echo "DROP TABLE IF EXISTS tab_00738 SYNC;
DROP TABLE IF EXISTS mv SYNC;
-- create table with fsync and 20 partitions for slower INSERT
-- (since increasing number of records will make it significantly slower in debug build, but not in release)
CREATE TABLE tab_00738(a Int) ENGINE = MergeTree() ORDER BY a PARTITION BY a%20 SETTINGS fsync_after_insert=1;
CREATE MATERIALIZED VIEW mv UUID '$uuid' ENGINE = Log AS SELECT a FROM tab_00738;" | ${CLICKHOUSE_CLIENT} -n

${CLICKHOUSE_CLIENT} --query_id insert_$CLICKHOUSE_DATABASE --query "INSERT INTO tab_00738 SELECT number FROM numbers(10000000)" &

function drop()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE \`.inner_id.$uuid\`" -n
}

function wait_for_query_to_start()
{
    while [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = 'insert_$CLICKHOUSE_DATABASE'") == 0 ]]; do sleep 0.001; done

    # The query is already started, but there is no guarantee that it locks the underlying table already.
    # Wait until PushingToViewsBlockOutputStream will acquire the lock of the underlying table for the INSERT query.
    # (assume that 0.5 second is enough for this, but this is not 100% correct)
    sleep 0.5

    # query already finished, fail
    if [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = 'insert_$CLICKHOUSE_DATABASE'") == 0 ]]; then
        exit 2
    fi
}

export -f wait_for_query_to_start
timeout 5 bash -c wait_for_query_to_start

drop &

wait

echo "DROP TABLE IF EXISTS tab_00738;
DROP TABLE IF EXISTS mv;" | ${CLICKHOUSE_CLIENT} -n
