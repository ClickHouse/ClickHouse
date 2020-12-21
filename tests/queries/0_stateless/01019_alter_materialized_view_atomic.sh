#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
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

    while true; do
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 -q "${ALTERS[$RANDOM % 2]}"
        sleep "$(echo 0.$RANDOM)";
    done
}

export -f alter_thread;
timeout 10 bash -c alter_thread &

for _ in {1..100}; do
    # Retry (hopefully retriable (deadlock avoided)) errors.
    while true; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (1);" 2>/dev/null && break
    done
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv;"
wait
