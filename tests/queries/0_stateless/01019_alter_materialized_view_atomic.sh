#!/usr/bin/env bash
# Tags: no-fasttest, no-debug

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS src_456;
DROP TABLE IF EXISTS mv_456;

CREATE TABLE src_456 (v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_456 (v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src_456;
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
#   mv_456 sink
#
# Insert N times into null while altering sink query and switching it from mv1 to mv2.

# The SIGNAL
INSERT_DONE=0

function alter_thread()
{
    # When INSERT done, we got signal
    trap 'INSERT_DONE=1' SIGINT SIGTERM

    ALTERS[0]="ALTER TABLE mv_456 MODIFY QUERY SELECT v FROM src_456;"
    ALTERS[1]="ALTER TABLE mv_456 MODIFY QUERY SELECT v * 2 as v FROM src_456;"

    # Loop to make the ALTER run concurrently with INSERT
    for i in {1..10}; do
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 -q "${ALTERS[$RANDOM % 2]}"
        sleep "$(echo 0.$RANDOM)";
    done

    # Wait until the INSERT is done
    while [[ $INSERT_DONE -eq 0 ]]; do
        sleep 0.1
    done

    # Insert done, DROP the tables
    $CLICKHOUSE_CLIENT -q "DROP VIEW mv_456"
    $CLICKHOUSE_CLIENT -q "DROP TABLE src_456"
}

export -f alter_thread;
alter_thread &
ALTER_DROP_PID=$!

for _ in {1..100}; do
    # Retry (hopefully retriable (deadlock avoided)) errors.
    while true; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO src_456 VALUES (1);" 2>/dev/null && break
    done
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_456;"

# After INSERT is done, notify the alter thread to stop and then we could drop tables
kill -SIGINT $ALTER_DROP_PID

# Wait until alter and drop finish
wait $ALTER_DROP_PID