#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

set -euo pipefail

# A `FREEZE` snapshot on a content-addressed disk belongs to the server root that made it. `UNFREEZE`
# is local and destructive, so releasing one root's freeze must not touch another root's.
#
# Two server roots sharing one pool is how two replicas of one table look from the pool's side. The
# destructive lookup needs ONE table path reachable from both roots, and the shadow path embeds the
# table UUID in an Atomic database -- so the UUID is reused sequentially rather than creating two
# tables, which would have two UUIDs, two namespaces, and no cross-root lookup to test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_UUID='05024aaa-0000-4000-8000-000000000001'
SHARED_BACKUP='shared_05024'
B_BACKUP='own_b_05024'
DISK_B='05024_cas_freeze_b'
UNFREEZE_STRUCTURE='command_type String, partition_id String, part_name String, backup_name String, backup_path String, part_backup_path String'

# `ALTER ... UNFREEZE` returns rows only under `alter_partition_verbose_result=1`; the default is off.
# `backup_path` and `part_backup_path` are absolute, so only the stable columns are printed.
unfreeze_and_print() {
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE $1 UNFREEZE WITH NAME '$2' SETTINGS alter_partition_verbose_result = 1;" \
        | ${CLICKHOUSE_LOCAL} --structure "$UNFREEZE_STRUCTURE" \
            --query "SELECT command_type, partition_id, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"
}

create_on_root() {
    # $1 = table name, $2 = `server_root_id`, $3 = disk name. One pool, two roots.
    ${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $1 UUID '${TABLE_UUID}' (k UInt32, v String)
    ENGINE = MergeTree ORDER BY k PARTITION BY k
    SETTINGS disk = disk(
        type = object_storage,
        object_storage_type = local,
        metadata_type = cas,
        server_root_id = '$2',
        name = '$3',
        path = '05024_cas_freeze_pool/',
        gc_enabled = 1,
        gc_interval_sec = 100000);"
}

drain_gc() {
    local pending=1
    for _ in $(seq 1 60); do
        pending=$(${CLICKHOUSE_CLIENT} --query "SYSTEM CAS GC RUN '${DISK_B}'" --format TSVWithNames \
            | awk -F'\t' 'NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                           { print $col["pending_condemned"] }')
        [ "$pending" = "0" ] && return
        sleep 0.5
    done
    echo "FAIL: GC did not drain within the bounded loop (pending=${pending})" >&2
    return 1
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_freeze_a;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_freeze_b;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_freeze_anchor;"

# Keep root B's disk alive for every collection round. This avoids relying on an inline disk remaining
# registered after its only table is dropped.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_cas_freeze_anchor (k UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05024_root_b',
    name = '${DISK_B}',
    path = '05024_cas_freeze_pool/',
    gc_enabled = 1,
    gc_interval_sec = 100000);"

# Root A freezes, then releases the UUID. Its freeze must outlive both the table and a collection round.
create_on_root t_cas_freeze_a 05024_root_a 05024_cas_freeze_a
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_freeze_a VALUES (1, 'a');"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cas_freeze_a FREEZE PARTITION 1 WITH NAME '${SHARED_BACKUP}';"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_freeze_a;"
drain_gc

# Root B reaches the SAME table path by reusing the UUID. Its own backup uses a distinct name: making
# both roots publish the same ref would mix two independent CAS writer lifecycles before `UNFREEZE`
# gets a chance to exercise the destructive lookup under test.
create_on_root t_cas_freeze_b 05024_root_b "${DISK_B}"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_freeze_b VALUES (1, 'b');"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cas_freeze_b FREEZE PARTITION 1 WITH NAME '${B_BACKUP}';"

# (1) B has no `shared_05024` freeze. Its foreign `UNFREEZE` must be a no-op; pre-fix it finds and
# drops A's pool-global shadow namespace and prints A's row here.
echo 'foreign_unfreeze'
unfreeze_and_print t_cas_freeze_b "${SHARED_BACKUP}"

# (2) B releases its OWN freeze. This catches a fix that scopes publication but leaves bulk lookup on
# the old unprefixed subtree.
echo 'unfreeze_b'
unfreeze_and_print t_cas_freeze_b "${B_BACKUP}"

# The foreign no-op must remain harmless after the retire pipeline reaches a fixpoint.
drain_gc

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_freeze_b;"

# (3) A's freeze must still be there. Recreate A's table on root A with the same UUID -- the freeze is
# addressed by path, so the recreated table reaches its predecessor's snapshot -- and release it.
# Pre-fix this prints nothing, because B's foreign unfreeze above already dropped the shared namespace.
create_on_root t_cas_freeze_a 05024_root_a 05024_cas_freeze_a
echo 'unfreeze_a'
unfreeze_and_print t_cas_freeze_a "${SHARED_BACKUP}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_freeze_a;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_freeze_anchor;"
${CLICKHOUSE_CLIENT} --query "SELECT 'dropped_ok';"
