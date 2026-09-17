#!/usr/bin/env bash
# Tags: long, no-parallel-replicas, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

$STREAMING_CLIENT -q "
    DROP TABLE IF EXISTS t_streaming_parts_removal;
    CREATE TABLE t_streaming_parts_removal (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS, old_parts_lifetime = 0;
    SYSTEM STOP MERGES t_streaming_parts_removal;
    INSERT INTO t_streaming_parts_removal SELECT number FROM numbers(20) SETTINGS min_insert_block_size_rows = 1, max_block_size = 1;
    SELECT 'active parts before truncate: ' || toString(count()) FROM system.parts WHERE database = currentDatabase() AND table = 't_streaming_parts_removal' AND active;
"

# The marker is the last row of the last block, so seeing it means the stream has read all data.
read -r fifo_1 pid_1 < <(spawn $STREAMING_CLIENT -q "SELECT a FROM t_streaming_parts_removal STREAM WHERE a = 19")
read_until "$fifo_1" "19"

$STREAMING_CLIENT -q "
    TRUNCATE TABLE t_streaming_parts_removal;
    SELECT 'active parts after truncate: ' || toString(count()) FROM system.parts WHERE database = currentDatabase() AND table = 't_streaming_parts_removal' AND active;
"

# The streaming query is still running here: outdated parts must not be pinned by its snapshot.
inactive=""
for _ in $(seq 1 300); do
    inactive=$($STREAMING_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_streaming_parts_removal'")
    [ "$inactive" = "0" ] && break
    sleep 0.2
done
echo "inactive parts after truncate: $inactive"

cleanup "$fifo_1" "$pid_1"

$STREAMING_CLIENT -q "DROP TABLE t_streaming_parts_removal"
