#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./merges.lib
. "$CURDIR"/merges.lib
set -e

$CLICKHOUSE_CLIENT --query "drop table if exists ttl;"

$CLICKHOUSE_CLIENT --query "create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d) ttl d + interval 1 day;"

$CLICKHOUSE_CLIENT --query "system stop ttl merges ttl;"

$CLICKHOUSE_CLIENT --query "insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1), (toDateTime('2000-10-10 00:00:00'), 2);"
$CLICKHOUSE_CLIENT --query "insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3), (toDateTime('2100-10-10 00:00:00'), 4);"

$CLICKHOUSE_CLIENT --query "optimize table ttl partition 10 final;"
$CLICKHOUSE_CLIENT --query "select * from ttl order by d, a;"

$CLICKHOUSE_CLIENT --query "system start ttl merges ttl;"
$CLICKHOUSE_CLIENT --query "optimize table ttl partition 10 final;"

wait_for_merges_done ttl

$CLICKHOUSE_CLIENT --query "select * from ttl order by d, a;"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl;"
