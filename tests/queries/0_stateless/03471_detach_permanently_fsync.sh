#!/usr/bin/env bash
# Tags: no-object-storage
# no-object-storage: object-storage disks do not fsync directories (DirectorySync stays 0)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# DETACH TABLE ... PERMANENTLY writes a `<table>.sql.detached` marker and ATTACH removes it.
# Both must fsync the metadata directory (under fsync_metadata) so the acknowledged transition
# survives a power loss. We can't cut power in a stateless test, so assert the DirectorySync
# ProfileEvent fires for the DETACH/ATTACH query (>=1 with fsync_metadata=1, 0 with =0), the
# same technique as 02361_fsync_profile_events.sh.

dir_sync() {
    # $1 = query_id
    $CLICKHOUSE_CLIENT --param_qid "$1" -q "
        SELECT ProfileEvents['DirectorySync']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id = {qid:String}
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE tbl (id UInt64) ENGINE = MergeTree ORDER BY id"

qid_detach="detach-${CLICKHOUSE_DATABASE}"
qid_attach="attach-${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$qid_detach" --fsync_metadata 1 -q "DETACH TABLE tbl PERMANENTLY"
$CLICKHOUSE_CLIENT --query_id "$qid_attach" --fsync_metadata 1 -q "ATTACH TABLE tbl"

# fsync_metadata = 0 must not fsync the directory, for both the marker create and remove sides.
qid_detach_off="detach-off-${CLICKHOUSE_DATABASE}"
qid_attach_off="attach-off-${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$qid_detach_off" --fsync_metadata 0 -q "DETACH TABLE tbl PERMANENTLY"
$CLICKHOUSE_CLIENT --query_id "$qid_attach_off" --fsync_metadata 0 -q "ATTACH TABLE tbl"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

echo -n "detach permanently, fsync_metadata=1: "
[[ "$(dir_sync "$qid_detach")" -ge 1 ]] && echo "DirectorySync >= 1" || echo "FAIL: no DirectorySync"
echo -n "attach, fsync_metadata=1: "
[[ "$(dir_sync "$qid_attach")" -ge 1 ]] && echo "DirectorySync >= 1" || echo "FAIL: no DirectorySync"
echo -n "detach permanently, fsync_metadata=0: "
echo "DirectorySync = $(dir_sync "$qid_detach_off")"
echo -n "attach, fsync_metadata=0: "
echo "DirectorySync = $(dir_sync "$qid_attach_off")"

$CLICKHOUSE_CLIENT -q "DROP TABLE tbl"
