#!/usr/bin/env bash
# Tags: distributed

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

# Attribute NETWORK_ERROR to this test's own query lineage via query_log instead of the
# process-wide system.errors counter, so unrelated concurrent queries elsewhere on the server
# cannot perturb the result (NETWORK_ERROR = 210).
network_errors=0
for ((i = 0; i < 100; ++i)); do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$i"

    opts=(
        "--max_distributed_connections=1"
        "--optimize_skip_unused_shards=1"
        "--optimize_distributed_group_by_sharding_key=1"
        "--prefer_localhost_replica=0"
    )
    # The query uses `FORMAT Null` to discard the output (we only care about NETWORK_ERROR side effects).
    # Do not pass `--format`: the `format` setting now takes precedence over the query `FORMAT` clause and would un-discard the output.
    $CLICKHOUSE_CLIENT "${opts[@]}" --query_id "$query_id" -m -q "select count(), * from dist_01247 group by number order by number limit 1 format Null"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    # expect zero new network errors attributed to this query (or its shard sub-queries)
    # shard-side sub-queries of the localhost cluster log current_database = 'default'
    network_errors=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE current_database IN (currentDatabase(), 'default') AND initial_query_id = '$query_id' AND exception_code = 210")

    if [[ $network_errors -eq 0 ]]; then
        break
    fi
done
echo NETWORK_ERROR=$network_errors

$CLICKHOUSE_CLIENT -q "drop table data_01247"
$CLICKHOUSE_CLIENT -q "drop table dist_01247"
