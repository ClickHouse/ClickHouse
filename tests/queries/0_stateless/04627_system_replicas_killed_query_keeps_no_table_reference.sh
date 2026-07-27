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

# Wait until the status request for our table exists. The query is paused in
# scheduleRequests by the failpoint, so the request cannot be processed yet.
found=0
for _ in {1..60}
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    found=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND query_id = '$query_id'
        AND message LIKE '%Making new request for table%t\\_status\\_no\\_pin%'")
    [[ "$found" -ge 1 ]] && break
    sleep 0.5
done
[[ "$found" -ge 1 ]] || echo "the status request did not appear in text_log"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT system_replicas_schedule_requests_pause"
wait

timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_status_no_pin SYNC" && echo "dropped"

# The abandoned request is drained by the next query over a still-existing table:
# it resolves to a dropped table and must not break the query.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_status_alive (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_status_alive', 'r1') ORDER BY k;
"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.replicas WHERE database = currentDatabase()"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_status_alive"

