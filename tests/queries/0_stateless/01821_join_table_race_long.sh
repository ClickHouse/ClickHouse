#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS join_table_race"
$CLICKHOUSE_CLIENT -q "CREATE TABLE join_table_race(id Int32, name String) ENGINE = Join(ANY, LEFT, id)"

# Under `async_insert` each `INSERT` waits for its flush. The adaptive busy timeout is tracked per queue shard,
# which is shared by all tables, so concurrent async inserts of other tests can hold it at
# `async_insert_busy_timeout_max_ms` (5 s in the test config) and 101 sequential inserts then take minutes.
for _ in {0..100}; do echo "INSERT INTO join_table_race VALUES ($RANDOM, '$RANDOM');"; done | $CLICKHOUSE_CLIENT --async_insert_use_adaptive_busy_timeout 0 --async_insert_busy_timeout_max_ms 100 --ignore-error -nm > /dev/null 2> /dev/null &

for _ in {0..200}; do echo "SELECT count() FROM join_table_race FORMAT Null;"; done | $CLICKHOUSE_CLIENT --ignore-error -nm > /dev/null 2> /dev/null &

for _ in {0..100}; do echo "TRUNCATE TABLE join_table_race;"; done | $CLICKHOUSE_CLIENT --ignore-error -nm > /dev/null 2> /dev/null &

for _ in {0..100}; do echo "ALTER TABLE join_table_race DELETE WHERE id % 2 = 0;"; done | $CLICKHOUSE_CLIENT --ignore-error -nm > /dev/null 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE join_table_race"
$CLICKHOUSE_CLIENT -q "INSERT INTO join_table_race VALUES (1, 'foo')"
$CLICKHOUSE_CLIENT -q "SELECT id, name FROM join_table_race"

$CLICKHOUSE_CLIENT -q "DROP TABLE join_table_race"
