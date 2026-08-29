#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A replica can finish a local part of a mutation while another part of the same mutation is still
# only a queue entry. The byte weight of the finished part must already be part of the mutation's
# denominator by then, or the first concrete `progress` reading - taken once the missing part
# arrives - is computed against that part alone and restarts from zero.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_partial_1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_partial_2 SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_partial_1 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_partial', '1') ORDER BY k"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_partial_2 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_partial', '2') ORDER BY k"

# The small part reaches the second replica; the large one is held back in its queue.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_partial_1 SELECT number, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_mut_partial_2"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP FETCHES t_mut_partial_2"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_partial_1 SELECT number, number FROM numbers(200000)"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_partial_1 UPDATE v = v + 1 WHERE 1" --mutations_sync=0

# Mutations are allowed on the second replica, so it rewrites the part it does have while the other
# one is still unfetched: parts_to_do drops to the single queued part, and progress stays unset
# because that part's size is still unknown here.
partial_state=""
for _ in {1..300}; do
    partial_state=$($CLICKHOUSE_CLIENT -q "
        SELECT parts_to_do, progress IS NULL
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_mut_partial_2' AND parts_to_do = 1 AND progress IS NULL")
    [ -n "$partial_state" ] && break
    sleep 0.3
done
echo "${partial_state:-FAIL: the local part never finished while the other was unfetched}"

# Freeze the mutation before the fetched part can be rewritten too, so the first concrete progress
# reading is taken with exactly the large part outstanding.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_mut_partial_2"
$CLICKHOUSE_CLIENT -q "SYSTEM START FETCHES t_mut_partial_2"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_mut_partial_2"

# The finished small part keeps its share of the denominator, so progress is above zero even though
# every byte still to do belongs to the part that just arrived.
first_reading=""
for _ in {1..300}; do
    first_reading=$($CLICKHOUSE_CLIENT -q "
        SELECT progress > 0
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_mut_partial_2' AND progress IS NOT NULL")
    [ -n "$first_reading" ] && break
    sleep 0.3
done
echo "${first_reading:-FAIL: progress never became concrete}"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_mut_partial_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_partial_1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_partial_2 SYNC"
