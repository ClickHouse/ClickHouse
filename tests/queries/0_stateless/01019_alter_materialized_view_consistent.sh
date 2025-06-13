#!/usr/bin/env bash
# Tags: no-fasttest, no-debug

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS src_123;
DROP TABLE IF EXISTS mv_123;

CREATE TABLE src_123 (v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_123 (v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src_123;
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
#   mv_123 sink
#
# Insert N times into null while altering sink query and switching it from mv1 to mv2.

function alter_thread()
{
    trap 'exit' INT

    ALTERS[0]="ALTER TABLE mv_123 MODIFY QUERY SELECT v FROM src_123;"
    ALTERS[1]="ALTER TABLE mv_123 MODIFY QUERY SELECT v * 2 as v FROM src_123;"

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
        $CLICKHOUSE_CLIENT -q "INSERT INTO src_123 VALUES (1);" 2>/dev/null && break
    done
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_123;"
wait

$CLICKHOUSE_CLIENT -q "DROP VIEW mv_123"
$CLICKHOUSE_CLIENT -q "DROP TABLE src_123"
