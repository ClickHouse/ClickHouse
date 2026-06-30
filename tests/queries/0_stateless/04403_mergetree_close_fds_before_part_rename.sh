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
# part has been committed to disk, the server holds no descriptor open *for writing* into
# it. We must restrict the check to write descriptors because read descriptors into a
# committed part are legitimate and expected -- prewarming the mark cache and the primary
# key cache (and the page cache, SELECTs, merges) open the very same files O_RDONLY right
# after the part is committed -- whereas only the writer's own leaked descriptors are the
# bug, and the writer always opens these files O_WRONLY.
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

# Stop background merges so a merge cannot open the part's files for writing and be
# mistaken for the writer's leaked descriptors.
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_fd_leak"

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_fd_leak'")

# Insert in the background; the server will pause for several seconds during the commit.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_fd_leak SELECT number, toString(number) FROM numbers(100)" >/dev/null 2>&1 &
insert_pid=$!

# While the insert is paused after the rename, look for any descriptor (across all
# processes) still open *for writing* into this table's committed part data files.
leaked=0
for _ in $(seq 1 200); do
    # 'find -lname' lists every '/proc/PID/fd/FD' symlink whose target points into this
    # table's part data/index/mark files (its glob matches '/' too, so "*$uuid*" spans
    # the store path).
    while read -r fd_link; do
        # Only a descriptor into a *committed* part is the bug. While a part is still being
        # written its data files live in a temporary directory ('tmp_insert_...') and the
        # writer legitimately holds them open for writing -- that is normal and is exactly
        # the descriptor that should be released by the rename. The bug is a descriptor that
        # *survives* the rename of that directory to its final committed name, so its /proc
        # symlink then resolves to the final path. Skip any target still pointing into a
        # temporary part directory; otherwise, on a slow (sanitizer) build under load, the
        # poll can catch the in-progress writer and report a spurious leak.
        target=$(readlink "$fd_link" 2>/dev/null) || continue
        case "$(basename "$(dirname "$target")")" in
            tmp_*|tmp-*|delete_tmp_*) continue ;;
        esac
        # /proc/PID/fdinfo/FD reports the open flags; the low two bits of the octal
        # 'flags' field are the access mode (0 = O_RDONLY, 1 = O_WRONLY, 2 = O_RDWR).
        # Ignore read-only descriptors (cache prewarming, the page cache, SELECTs).
        flags=$(grep -m1 '^flags:' "${fd_link/\/fd\//\/fdinfo\/}" 2>/dev/null | tr -dc '0-7')
        [ -n "$flags" ] || continue
        if [ $(( flags & 3 )) -ne 0 ]; then
            leaked=1
            break
        fi
    done < <(find /proc/[0-9]*/fd -mindepth 1 -maxdepth 1 \( \
                    -lname "*${uuid}*/data.bin" \
                 -o -lname "*${uuid}*/primary.cidx" \
                 -o -lname "*${uuid}*/primary.idx" \
                 -o -lname "*${uuid}*.cmrk[0-9]*" \) 2>/dev/null)
    [ "$leaked" -eq 1 ] && break
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
