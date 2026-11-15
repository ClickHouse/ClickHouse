#!/usr/bin/env bash
# Tags: no-replicated-database, no-object-storage, no-encrypted-storage
# Tag no-replicated-database: FREEZE is an unsupported type of ALTER query there.
# Tag no-object-storage, no-encrypted-storage: the test inspects part files on the local filesystem.

# Regression test for losing `invalidated_system_columns.txt` on the direct `freeze` paths of
# packed part storage. The file is written separately, next to `data.packed`, so
# `DataPartStorageOnDiskPacked::freeze` used to carry only the archive and dropped the file when
# the caller had no invalidated columns of its own (`freezePartitionsByMatcher` on
# `ALTER TABLE ... FREEZE`, `makeCloneInDetached` on `DETACH`). The frozen/detached copy of a part
# adopted from another table then kept the stale physically stored `_block_number`/`_block_offset`
# values without the disclaimer. `Full` part storage inherits the file by hardlinking every file of
# the part; both storage types are checked.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE_NAME="invalidated_system_columns.txt"

function check_storage()
{
    local full_part_storage_bytes=$1
    local label=$2

    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_inv_freeze_src;
        DROP TABLE IF EXISTS t_inv_freeze_dst;

        CREATE TABLE t_inv_freeze_src (p UInt8, x UInt64, y UInt64)
        ENGINE = MergeTree PARTITION BY p ORDER BY x
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                 min_bytes_for_full_part_storage = '$full_part_storage_bytes';

        CREATE TABLE t_inv_freeze_dst (p UInt8, x UInt64, y UInt64)
        ENGINE = MergeTree PARTITION BY p ORDER BY x
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                 min_bytes_for_full_part_storage = '$full_part_storage_bytes';

        INSERT INTO t_inv_freeze_src VALUES (1, 1, 0);
        INSERT INTO t_inv_freeze_src VALUES (1, 2, 0);

        -- Persist _block_number/_block_offset physically (they are written on a real merge).
        OPTIMIZE TABLE t_inv_freeze_src PARTITION 1 FINAL;

        -- Two adopted clones of the same source part; each gets invalidated_system_columns.txt,
        -- which regenerates the identities, so they stay unique within the table.
        ALTER TABLE t_inv_freeze_dst REPLACE PARTITION 1 FROM t_inv_freeze_src;
        ALTER TABLE t_inv_freeze_dst ATTACH PARTITION 1 FROM t_inv_freeze_src;

        SELECT DISTINCT part_storage_type FROM system.parts
        WHERE database = currentDatabase() AND table = 't_inv_freeze_dst' AND active;
    "

    local data_dir
    data_dir=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_inv_freeze_dst'")

    # The frozen copy of each adopted part must keep the invalidated-columns file.
    local with_file=0
    local part_backup_path
    while read -r part_backup_path; do
        if [ -f "$part_backup_path/$FILE_NAME" ]; then
            with_file=$((with_file + 1))
        fi
    done < <(${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_inv_freeze_dst FREEZE WITH NAME 'test_lwu57_$label' FORMAT TSV SETTINGS alter_partition_verbose_result = 1" \
        | ${CLICKHOUSE_LOCAL} --structure "command_type String, partition_id String, part_name String, backup_name String, backup_path String, part_backup_path String" \
            --query "SELECT part_backup_path FROM table")
    echo "frozen copies with the file: $with_file"

    # The detached clone of each adopted part must keep the invalidated-columns file as well.
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_inv_freeze_dst DETACH PARTITION 1"
    find "$data_dir/detached" -name "$FILE_NAME" | wc -l

    # Attaching back must keep the row identities unique.
    ${CLICKHOUSE_CLIENT} -q "
        ALTER TABLE t_inv_freeze_dst ATTACH PARTITION 1;
        SELECT count() == uniqExact(_block_number, _block_offset) FROM t_inv_freeze_dst;

        DROP TABLE t_inv_freeze_dst;
        DROP TABLE t_inv_freeze_src;
    "
}

check_storage 0 "full"
check_storage "100G" "packed"
