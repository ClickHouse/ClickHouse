#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} -q "drop table if exists ttl;"

${CLICKHOUSE_CLIENT} -q "create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d);"
${CLICKHOUSE_CLIENT} -q "insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1);"
${CLICKHOUSE_CLIENT} -q "insert into ttl values (toDateTime('2000-10-10 00:00:00'), 2);"
${CLICKHOUSE_CLIENT} -q "insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3);"
${CLICKHOUSE_CLIENT} -q "insert into ttl values (toDateTime('2100-10-10 00:00:00'), 4);"

${CLICKHOUSE_CLIENT} -q "alter table ttl materialize ttl" --server_logs_file=/dev/null 2>&1 | grep -o "Cannot MATERIALIZE TTL"

${CLICKHOUSE_CLIENT} -q "alter table ttl modify ttl d + interval 1 day"

${CLICKHOUSE_CLIENT} -q "alter table ttl materialize ttl"

wait_for_mutation "ttl" "mutation_5.txt"

${CLICKHOUSE_CLIENT} -q "select * from ttl order by a"

${CLICKHOUSE_CLIENT} -q "drop table if exists ttl"
