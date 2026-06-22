#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest, no-shared-merge-tree
# Tag no-replicated-database: relies on a single local replica and reads its /proc fds
# Tag no-shared-merge-tree: SharedMergeTree does not honor sleep_before_commit_local_part_in_replicated_table_ms

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/56288
#
# The MergeTree part writer used to keep the data ('data.bin'), marks ('data.cmrk*')
# and primary index ('primary.cidx') file descriptors open until the writer object was
# destroyed, which happened only after the part's temporary directory had already been
# renamed to its final name. Renaming a directory that still has open file descriptors
# inside fails on filesystems backed by Windows (WSL, CIFS/SMB, Docker Desktop bind
# mounts), breaking INSERT. On ext4/XFS the rename succeeds regardless, so we cannot
# reproduce the failure directly; instead we assert the invariant that fixes it: once a
# part has been committed to disk, the server holds no open descriptors into it.
#
# `sleep_before_commit_local_part_in_replicated_table_ms` pauses the insert *after* the
# part directory has been renamed but *before* the temporary part (which owns the writer
# and its descriptors) is destroyed -- exactly the window in which the leaked descriptors
# used to be observable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_fd_leak SYNC"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_fd_leak (a UInt64, b String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fd_leak', 'r1')
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,  -- force a compact part (single data.bin + data.cmrk*)
    min_rows_for_wide_part = 1000000000,
    sleep_before_commit_local_part_in_replicated_table_ms = 6000
"

# No background merges, so the only descriptors that could point into the part are the
# writer's own (a read for a merge would otherwise be a false positive).
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_fd_leak"

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_fd_leak'")

# Insert in the background; the server will pause for several seconds during the commit.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_fd_leak SELECT number, toString(number) FROM numbers(100)" >/dev/null 2>&1 &
insert_pid=$!

# While the insert is paused after the rename, look for any open descriptor (across all
# processes) that still points into this table's committed part data files.
leaked=0
for _ in $(seq 1 200); do
    if ls -l /proc/[0-9]*/fd/ 2>/dev/null \
        | grep -F -- "$uuid" \
        | grep -Eq -- '-> .*/(data\.bin|primary\.cidx|primary\.idx|[^/]*\.cmrk[0-9]+)$'
    then
        leaked=1
        break
    fi
    # Stop once the insert has finished: the observation window is closed.
    kill -0 "$insert_pid" 2>/dev/null || break
    sleep 0.1
done

wait "$insert_pid" 2>/dev/null

if [ "$leaked" -eq 0 ]; then
    echo "no leaked descriptors"
else
    echo "leaked descriptors found"
fi

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_fd_leak"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_fd_leak SYNC"
