#!/usr/bin/env bash
# An ALTER that lands while the source is still being planned must not slip past insert_expected_column_types.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

marker="05099_alter_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_expected_alter; CREATE TABLE t_expected_alter (x UInt64) ENGINE = Memory"

# The url() schema inference runs this slow query while the INSERT is planned, after it has checked the table.
src="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+300+AS+x,+%27${marker}%27+AS+m+FROM+system.one+WHERE+sleepEachRow(3)=0+FORMAT+TSVWithNames"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_expected_alter SELECT x FROM url('$src', TSVWithNames)
    SETTINGS insert_expected_column_types = {'x': 'UInt64'}, use_structure_from_insertion_table_in_table_functions = 0" 2>&1 | grep -o -m1 'INCOMPATIBLE_SCHEMA' &

for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query LIKE '%${marker}%sleepEachRow%' AND query NOT LIKE '%INSERT%' AND query NOT LIKE '%system.processes%'")" = "1" ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_expected_alter MODIFY COLUMN x UInt8"
wait

$CLICKHOUSE_CLIENT -q "SELECT 'rows', count() FROM t_expected_alter"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_expected_alter"
