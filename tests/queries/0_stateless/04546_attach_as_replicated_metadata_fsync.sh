#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage
# no-object-storage: fsync of a directory is a local-filesystem operation; object storage
#   disks return no directory sync guard, so DirectorySync stays 0 there.
#
# ATTACH TABLE ... AS [NOT] REPLICATED must make its on-disk changes power-loss durable when
# fsync_metadata is on: the metadata `.sql` rename, and (converting to replicated) the removal
# of every part's txn_version.txt(.tmp). Both are directory-entry changes and require the
# containing directory to be fdatasync'd. The exact DirectorySync counts are asserted so the
# test fails if either the metadata-directory guard or the per-part guard is removed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

directory_sync_of() {
    # DirectorySync count for a single ATTACH query, keyed by query_id, scoped to this test's database.
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT ProfileEvents['DirectorySync']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id = '$1'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

# ---- Converting MergeTree -> ReplicatedMergeTree ---------------------------------------------
# clearTransactionMetadata removes each part's txn_version.txt(.tmp), then the metadata .sql is
# renamed. With fsync_metadata = 1 every modified part directory plus the metadata directory is
# fdatasync'd, so with two parts carrying txn files DirectorySync = 2 (parts) + 1 (metadata) = 3.
# The exact count fails if the per-part guard is removed (would drop to 1) or the metadata guard
# is removed (would drop to 2). With fsync_metadata = 0 nothing is synced.
run_to_replicated() {
    local fsync=$1
    local qid="04546-repl-${fsync}-${CLICKHOUSE_DATABASE}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_repl SYNC"
    # Two independent parts, background merges off so they stay separate.
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_repl (n Int64) ENGINE = MergeTree ORDER BY n SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_repl"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_repl VALUES (1)"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_repl VALUES (2)"

    # Plant txn metadata files so clearTransactionMetadata has files to remove in each part dir
    # (mirrors 04492; a committed part legitimately carries txn_version.txt when transactions are
    # used, and a leftover txn_version.txt.tmp can linger). Both filenames are exercised; the
    # per-part guard is acquired once per part regardless.
    local first=1
    while read -r part; do
        [[ -z "$part" ]] && continue
        echo "CSN: 1" > "${part}/txn_version.txt"
        if [[ "$first" == "1" ]]; then
            echo "incomplete" > "${part}/txn_version.txt.tmp"
            first=0
        fi
    done < <(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_repl' AND active")

    local nparts=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_repl' AND active")
    ${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_repl SYNC"
    ${CLICKHOUSE_CLIENT} --fsync_metadata "$fsync" --query_id "$qid" -q "ATTACH TABLE t_repl AS REPLICATED"

    local synced=$(directory_sync_of "$qid")
    if [[ "$fsync" == "1" ]]; then
        # Expect one fdatasync per modified part directory plus one for the metadata directory.
        local expected=$((nparts + 1))
        [[ "$synced" == "$expected" ]] && echo "to_replicated fsync_metadata=1: synced parts+metadata" || echo "to_replicated fsync_metadata=1: WRONG sync count (got $synced, want $expected)"
    else
        [[ "$synced" == "0" ]] && echo "to_replicated fsync_metadata=0: not synced" || echo "to_replicated fsync_metadata=0: unexpectedly synced ($synced)"
    fi
    echo "to_replicated fsync_metadata=$fsync count $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_repl")"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_repl SYNC"
}

# ---- Converting ReplicatedMergeTree -> MergeTree ---------------------------------------------
# Only the metadata .sql rename happens here (no txn-file removals), so exactly the metadata
# directory is fdatasync'd: DirectorySync = 1 with fsync_metadata = 1, and 0 with it disabled.
run_to_not_replicated() {
    local fsync=$1
    local qid="04546-notrepl-${fsync}-${CLICKHOUSE_DATABASE}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_nrepl SYNC"
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_nrepl (n Int64) ENGINE = MergeTree ORDER BY n"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_nrepl VALUES (1)"
    ${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_nrepl SYNC"
    ${CLICKHOUSE_CLIENT} --fsync_metadata 0 -q "ATTACH TABLE t_nrepl AS REPLICATED"
    ${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_nrepl SYNC"
    ${CLICKHOUSE_CLIENT} --fsync_metadata "$fsync" --query_id "$qid" -q "ATTACH TABLE t_nrepl AS NOT REPLICATED"

    local synced=$(directory_sync_of "$qid")
    if [[ "$fsync" == "1" ]]; then
        [[ "$synced" == "1" ]] && echo "to_not_replicated fsync_metadata=1: metadata synced" || echo "to_not_replicated fsync_metadata=1: WRONG sync count (got $synced, want 1)"
    else
        [[ "$synced" == "0" ]] && echo "to_not_replicated fsync_metadata=0: not synced" || echo "to_not_replicated fsync_metadata=0: unexpectedly synced ($synced)"
    fi
    echo "to_not_replicated fsync_metadata=$fsync count $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_nrepl")"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_nrepl SYNC"
}

run_to_replicated 1
run_to_replicated 0
run_to_not_replicated 1
run_to_not_replicated 0
