#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage
#
# Regression test for the startup-recovery path of the `cleanup` helper shared by the
# `part_loading_tree_rollback` tests (04241/04063/04417/04500).
#
# Those tests fabricate part directories out-of-band while the table is `DETACH`ed
# `PERMANENTLY`. If a previous attempt exits after `DETACH TABLE ... PERMANENTLY` but before it
# re-attaches, it leaves a permanently detached table whose name blocks `CREATE TABLE` and
# whose fabricated `txn_version.txt` can make a plain `ATTACH TABLE` throw. The next run's
# leading `cleanup` must recover from such a leak even though its in-shell `DATA_PATH` is not
# yet set: it locates the data directory from the detached table's own `uuid`
# (`<disk>/store/<uuid[0:3]>/<uuid>`), removes the fabricated parts, re-attaches and drops the
# table, so the following `CREATE TABLE` succeeds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_plt_startup_recovery"
FABRICATED_PARTS="all_1_2_1_0 all_3_4_1_0"

# The recovery logic used by the rollback tests' `cleanup`: it derives the data path from the
# detached table's `uuid`, so it works regardless of whether the in-shell `DATA_PATH` is set.
recover_leaked_table()
{
    local detached_uuid disk_path data_path part
    detached_uuid=$($CLICKHOUSE_CLIENT -q "
        SELECT uuid FROM system.detached_tables
        WHERE database = currentDatabase() AND table = '${TABLE}'" 2>/dev/null)
    if [ -n "${detached_uuid}" ]; then
        disk_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'" 2>/dev/null)
        if [ -n "${disk_path}" ]; then
            data_path="${disk_path}store/${detached_uuid:0:3}/${detached_uuid}"
            for part in ${FABRICATED_PARTS}; do
                rm -rf "${data_path:?}/${part}"
            done
        fi
        $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${TABLE}" 2>/dev/null
    fi
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null
}
trap recover_leaked_table EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"

# --- Simulate a previous failed attempt that leaked a permanently detached table. ---
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (42)"
DATA_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT data_paths[1] FROM system.tables
    WHERE database = currentDatabase() AND name = '${TABLE}'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${TABLE} PERMANENTLY"

# Leave a fabricated part directory with a corrupted `txn_version.txt` behind, exactly the kind
# of leftover that could make a plain `ATTACH TABLE` throw.
cp -r "${DATA_PATH}all_1_1_0" "${DATA_PATH}all_1_2_1_0"
printf 'version: 1\nversion: 1\n' > "${DATA_PATH}all_1_2_1_0/txn_version.txt"

# The leaked table is present in `system.detached_tables` (prints 1).
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_tables
    WHERE database = currentDatabase() AND table = '${TABLE}'"

# --- Recover with the in-shell `DATA_PATH` unset, as it is during the next run's startup. ---
unset DATA_PATH
recover_leaked_table

# The table must be gone from both catalogs (both print 0)...
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = '${TABLE}'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_tables
    WHERE database = currentDatabase() AND table = '${TABLE}'"

# ...and the name must be free for a fresh `CREATE TABLE`.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} (x UInt32) ENGINE = MergeTree ORDER BY x"
echo OK
