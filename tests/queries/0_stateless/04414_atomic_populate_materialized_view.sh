#!/usr/bin/env bash
# Tags: long

# Test that CREATE MATERIALIZED VIEW ... POPULATE is atomic: rows inserted into the source table
# concurrently with the population are delivered to the view exactly once - neither missed nor
# duplicated. Before the fix, such rows could be lost (routed nowhere) or duplicated (both routed
# to the view and present in the population snapshot). Covers both plain CREATE and CREATE OR REPLACE.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $1 - the CREATE statement to run (POPULATE), $2 - the range start of the concurrent insert.
# Starts the population in the background; merge_tree_storage_snapshot_sleep_ms widens the
# exclusive-lock + snapshot window so the concurrent insert reliably collides with it. Whatever the
# interleaving, every row must end up in the view exactly once.
function run_round()
{
    local create_query=$1
    local insert_start=$2

    $CLICKHOUSE_CLIENT --merge_tree_storage_snapshot_sleep_ms=1000 -q "$create_query" &
    local create_pid=$!

    sleep 1
    $CLICKHOUSE_CLIENT -q "INSERT INTO src SELECT number FROM numbers($insert_start, 100000)"

    wait $create_pid

    $CLICKHOUSE_CLIENT -m -q "
    SELECT
        (SELECT count() FROM mv) = (SELECT count() FROM src) AS no_missing_no_extra,
        (SELECT count() FROM mv) = (SELECT uniqExact(id) FROM mv) AS no_duplicates;
    "
}

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(100000);
"

# Plain CREATE ... POPULATE.
run_round "CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src" 100000

# CREATE OR REPLACE ... POPULATE goes through a different (atomic-swap) code path; it must be atomic too.
run_round "CREATE OR REPLACE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src" 200000

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE mv;
DROP TABLE src;
"
