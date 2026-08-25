#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A local source satisfies the `LIMIT` and closes the remote sources' output ports before their
# queries have been sent. `RemoteQueryExecutor::finish` marks such an executor `finished`; it must
# also mark it cancelled, because both `sendQuery` and `sendQueryAsync` gate only on
# `was_cancelled`. Otherwise a `RemoteSource::work()` that was already scheduled sends the query
# after the executor declared itself finished, and nothing releases it afterwards - `finish`
# returns early from then on and the destructor does not disconnect, so the connection goes back
# to the pool while the remote query is still running.
#
# This is not specific to parallel replicas, which is why it is checked on a plain `remote()`:
# `async_query_sending_for_remote = 0` sends from `work()` synchronously, so `finish()` observes an
# executor that has neither sent its query nor created a read context.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_04605 SYNC;
    CREATE TABLE t_04605 (x String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_04605 SELECT toString(number) FROM numbers(10);
"

query_id="04605_${CLICKHOUSE_DATABASE}_$RANDOM"

$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT x FROM
    (
        SELECT '0' AS x
        UNION ALL
        SELECT x FROM remote('127.0.0.{2,3,4}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), t_04605)
    )
    LIMIT 1
    FORMAT Null
    SETTINGS max_threads = 1, async_query_sending_for_remote = 0
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# The local source alone satisfies the LIMIT, so no remote read should ever have been started.
# A non-zero count means a query was sent for an executor that had already been marked finished.
# Scoping is by `initial_query_id`, which is unique to this run; `databases` additionally keeps the
# lookup within this test's own database. `current_database` is not usable here - a remote query
# runs with `default` as its current database - and `databases` is empty for the `DESC TABLE` that
# `remote()` issues to infer the structure, so that one does not count either.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.query_log
    WHERE initial_query_id = '$query_id' AND query_id != initial_query_id
      AND type = 'QueryStart' AND event_date >= yesterday()
      AND has(databases, currentDatabase())
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04605 SYNC"
