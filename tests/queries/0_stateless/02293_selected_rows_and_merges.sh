#!/usr/bin/env bash
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} -q "create table tt (x UInt32, y UInt32) engine = MergeTree order by x"
${CLICKHOUSE_CLIENT} -q "insert into tt select number, 0 from numbers(1e6)"
${CLICKHOUSE_CLIENT} -q "insert into tt select number, 1 from numbers(1e6)"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "optimize table tt final" "--query_id=$query_id"

# Here SelectRows and SelectBytes should be zero, MergedRows is 2m and MergedUncompressedBytes is 16m
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select ProfileEvents['SelectedRows'], ProfileEvents['SelecteBytes'], ProfileEvents['MergedRows'], ProfileEvents['MergedUncompressedBytes'] from system.query_log where query_id = '$query_id' and type = 'QueryFinish' and query like 'optimize%' and current_database = currentDatabase()"

${CLICKHOUSE_CLIENT} --mutations_sync 1 -q "alter table tt update y = y + 1 where 1" "--query_id=$query_id"
${CLICKHOUSE_CLIENT} -q "system flush logs"

# Here for mutation all values are 0, cause mutation is executed async.
# It's pretty hard to write a test with total counter.
${CLICKHOUSE_CLIENT} -q "select ProfileEvents['SelectedRows'] > 10, ProfileEvents['SelecteBytes'], ProfileEvents['MergedRows'], ProfileEvents['MergedUncompressedBytes'] from system.query_log where query_id = '$query_id' and type = 'QueryFinish' and query like 'alter%' and current_database = currentDatabase()"


