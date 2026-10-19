#!/usr/bin/env bash
# Tags: no-fasttest, no-debug, no-flaky-check
# no-flaky-check: some queries are too long with Thread Fuzzer

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv (v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src;
EOF

# Test that ALTER doesn't cause data loss or duplication.
#
# Idea for future:
#
#    null
#  /      \
# mv1    mv2
#  \      /
#   \    /
#   mv sink
#
# Insert N times into null while altering sink query and switching it from mv1 to mv2.

function alter_thread()
{
    trap 'exit' INT

    ALTERS[0]="ALTER TABLE mv MODIFY QUERY SELECT v FROM src;"
    ALTERS[1]="ALTER TABLE mv MODIFY QUERY SELECT v * 2 as v FROM src;"

    local TIMELIMIT=$((SECONDS+10))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 -q "${ALTERS[$RANDOM % 2]}"
        sleep "$(echo 0.$RANDOM)";
    done
}

alter_thread &

# Under `async_insert` each `INSERT` waits for its flush. The adaptive busy timeout is tracked per queue shard,
# which is shared by all tables, so concurrent async inserts of other tests can hold it at
# `async_insert_busy_timeout_max_ms` (5 s in the test config) and 100 sequential inserts then take minutes.
for _ in {1..100}; do
    # Retry (hopefully retriable (deadlock avoided)) errors.
    while true; do
        $CLICKHOUSE_CLIENT --async_insert_use_adaptive_busy_timeout 0 --async_insert_busy_timeout_max_ms 100 -q "INSERT INTO src VALUES (1);" 2>/dev/null && break
    done
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv;"
wait

$CLICKHOUSE_CLIENT -q "DROP VIEW mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE src"
