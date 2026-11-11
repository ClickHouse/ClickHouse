#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: the failpoint pauses every concurrent system.replicas query on the server.
# no-replicated-database: the replica name is fixed and the failpoint is enabled on one replica only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query over system.replicas killed before its status requests are scheduled must not
# keep references to the storages: a reference leaked in StatusRequestsPool would make
# the following DROP TABLE SYNC hang forever.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_status_no_pin (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_status_no_pin', 'r1') ORDER BY k;
    SYSTEM ENABLE FAILPOINT system_replicas_schedule_requests_pause;
"

query_id="${CLICKHOUSE_DATABASE}_replicas_to_kill"
$CLICKHOUSE_CLIENT --query_id "$query_id" --max_execution_time 60 -q "
    SELECT * FROM system.replicas WHERE database = currentDatabase() FORMAT Null
" >/dev/null 2>&1 &

# The request for the table is created before scheduling starts, so once the query is
# parked at the failpoint the request is queued and cannot be processed.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT system_replicas_schedule_requests_pause PAUSE"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"

# Resume the query only after the kill is visible to it, so that it abandons the request.
cancelled=0
for _ in {1..300}
do
    cancelled=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND is_cancelled")
    [[ "$cancelled" -ge 1 ]] && break
    sleep 0.1
done
[[ "$cancelled" -ge 1 ]] || echo "the query was not cancelled"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT system_replicas_schedule_requests_pause"
wait

timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_status_no_pin SYNC" && echo "dropped"

# The abandoned request is drained by the next query over a still-existing table:
# it resolves to a dropped table and must not break the query. The query has to read
# total_replicas, otherwise it uses the pool that holds no abandoned request.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_status_alive (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_status_alive', 'r1') ORDER BY k;
"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT total_replicas FROM system.replicas WHERE database = currentDatabase())"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_status_alive"

