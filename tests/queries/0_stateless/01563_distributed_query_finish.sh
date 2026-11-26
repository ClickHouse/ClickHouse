#!/usr/bin/env bash
# Tags: distributed, no-fasttest
# Tag no-fasttest: checks system.text_log

# query finish should not produce any NETWORK_ERROR
# (NETWORK_ERROR will be in case of connection reset)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m <<EOL
drop table if exists dist_01247;
drop table if exists data_01247;

create table data_01247 engine=Memory() as select * from numbers(2);
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, '$CLICKHOUSE_DATABASE', data_01247, number);

select * from dist_01247 format Null;
EOL

# NETWORK_ERROR of our own query via system.text_log (append-only, keyed by query_id):
# noise-immune both ways vs last-writer-wins system.errors. Per-run salt skips stale rerun
# rows; the event_date/event_time lower bound prunes text_log by its sort key so the retry
# loop cannot degrade into repeated full scans; enable_parallel_replicas=0 keeps read local.
salt=$(random_str 10)
run_start=$($CLICKHOUSE_CLIENT -q "SELECT now()")
network_errors=0
for ((i = 0; i < 100; ++i)); do
    query_id="01563_distributed_query_finish-$CLICKHOUSE_DATABASE-$salt-$i"

    opts=(
        "--max_distributed_connections=1"
        "--optimize_skip_unused_shards=1"
        "--optimize_distributed_group_by_sharding_key=1"
        "--prefer_localhost_replica=0"
        "--query_id=$query_id"
    )
    # The query uses `FORMAT Null` to discard the output (we only care about NETWORK_ERROR side effects).
    # Do not pass `--format`: the `format` setting now takes precedence over the query `FORMAT` clause and would un-discard the output.
    $CLICKHOUSE_CLIENT "${opts[@]}" -m -q "select count(), * from dist_01247 group by number order by number limit 1 format Null"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    network_errors=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.text_log WHERE event_date >= toDate('$run_start') AND event_time >= '$run_start' AND query_id = '$query_id' AND message LIKE '%NETWORK_ERROR%' SETTINGS enable_parallel_replicas = 0")

    if [[ $network_errors -eq 0 ]]; then
        break
    fi
done
echo NETWORK_ERROR=$network_errors

$CLICKHOUSE_CLIENT -q "drop table data_01247"
$CLICKHOUSE_CLIENT -q "drop table dist_01247"
