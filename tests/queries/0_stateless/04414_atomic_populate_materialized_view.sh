#!/usr/bin/env bash
# Tags: long

# Test that CREATE MATERIALIZED VIEW ... POPULATE is atomic: rows inserted into the source table
# concurrently with the population are delivered to the view exactly once - neither missed nor
# duplicated. Before the fix, such rows could be lost (routed nowhere) or duplicated (both routed
# to the view and present in the population snapshot).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(100000);
"

# Start the atomic POPULATE in the background. The source snapshot is captured under a brief
# exclusive lock on the source table; merge_tree_storage_snapshot_sleep_ms widens that window so the
# concurrent insert below reliably collides with it.
$CLICKHOUSE_CLIENT --merge_tree_storage_snapshot_sleep_ms=1000 -q "
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src
" &
CREATE_PID=$!

# Give the CREATE time to reach the exclusive-lock + snapshot window, then insert concurrently.
# Whatever the interleaving, every row must end up in the view exactly once.
sleep 1
$CLICKHOUSE_CLIENT -q "INSERT INTO src SELECT number FROM numbers(100000, 100000)"

wait $CREATE_PID

$CLICKHOUSE_CLIENT -m -q "
SELECT
    (SELECT count() FROM mv) = (SELECT count() FROM src) AS no_missing_no_extra,
    (SELECT count() FROM mv) = (SELECT uniqExact(id) FROM mv) AS no_duplicates;
DROP TABLE mv;
DROP TABLE src;
"
