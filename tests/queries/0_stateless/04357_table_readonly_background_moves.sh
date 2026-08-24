#!/usr/bin/env bash
# Tags: long, no-fasttest, no-replicated-database, no-shared-merge-tree
# ^ long: waits for the background move pool to make progress within a bounded time window.
#   no-fasttest: the `local_remote` storage policy uses an S3-backed volume (requires MinIO).
#   no-replicated-database, no-shared-merge-tree: the `table_readonly` setting is rejected on
#   ReplicatedMergeTree/SharedMergeTree, so this test only applies to a plain (non-replicated) MergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read-only table (the `table_readonly` MergeTree setting, used e.g. for rotated system log tables)
# must not waste background CPU and I/O moving parts between volumes. A writable control table proves
# the background move pool is actively making progress; the read-only table must keep its part on the
# original (local) volume.

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS t_ro_move;
DROP TABLE IF EXISTS t_w_move;

-- 'TTL ... TO VOLUME' with a small delay: the part is first written to the 'local' volume and only
-- becomes eligible for a background move to the 'remote' volume a few seconds later. now() is an
-- absolute instant, so a randomized session_timezone does not shift this boundary.
CREATE TABLE t_ro_move (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 5 SECOND TO VOLUME 'remote'
SETTINGS storage_policy = 'local_remote';
CREATE TABLE t_w_move (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 5 SECOND TO VOLUME 'remote'
SETTINGS storage_policy = 'local_remote';

-- Stop moves so the part is not moved before the table is marked read-only. The move observed below
-- on the writable table can then only happen through the (resumed) background move scheduling path.
SYSTEM STOP MOVES t_ro_move;
SYSTEM STOP MOVES t_w_move;

INSERT INTO t_ro_move VALUES (now(), 1);
INSERT INTO t_w_move VALUES (now(), 1);
"

# Both parts must start on the local volume (disk 'default'): the move TTL has not expired yet at insert.
echo "initial readonly disk:"
${CLICKHOUSE_CLIENT} -q "SELECT disk_name FROM system.parts WHERE database = currentDatabase() AND table = 't_ro_move' AND active"

${CLICKHOUSE_CLIENT} -m -q "
-- Mark the first table as read-only, then allow moves again on both.
ALTER TABLE t_ro_move MODIFY SETTING table_readonly = 1;
SYSTEM START MOVES t_ro_move;
SYSTEM START MOVES t_w_move;
"

# Wait until the writable control table's part is moved to the remote volume (disk 's3_disk') in the
# background. This proves the background move pool is actively making progress right now.
moved=0
for _ in $(seq 1 120); do
    disk=$(${CLICKHOUSE_CLIENT} -q "SELECT disk_name FROM system.parts WHERE database = currentDatabase() AND table = 't_w_move' AND active")
    if [[ "$disk" == "s3_disk" ]]; then
        moved=1
        break
    fi
    sleep 0.5
done

if [[ "$moved" -eq 1 ]]; then
    echo "writable control moved to remote: OK"
else
    echo "writable control moved to remote: FAIL (disk: $(${CLICKHOUSE_CLIENT} -q "SELECT disk_name FROM system.parts WHERE database = currentDatabase() AND table = 't_w_move' AND active"))"
fi

# The read-only table's part must NOT have been moved: no background move should have been scheduled.
echo "readonly disk after control moved:"
${CLICKHOUSE_CLIENT} -q "SELECT disk_name FROM system.parts WHERE database = currentDatabase() AND table = 't_ro_move' AND active"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE t_ro_move;
DROP TABLE t_w_move;
"
