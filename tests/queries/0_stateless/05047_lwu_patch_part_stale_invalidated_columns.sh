#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage
# The test writes a file directly into the part directory on the local disk.
# min_bytes_for_full_part_storage = 0: a packed part keeps its files inside the
# single data.packed archive, so the planted file would not be read from the disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Servers before the fix wrote `invalidated_system_columns.txt` with `_block_number`
# and `_block_offset` on ATTACH PARTITION and RESTORE into every part, including patch
# parts, where these columns are the payload. A patch part with such file returned a
# null `_block_number` column and crashed reads after every reload of the part.
# Verify that the generic part-loading path ignores the stale file for patch parts:
# plant the file into an active patch part and reload the table.

for version in v1 v2; do
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS t_patch_stale_file;
        CREATE TABLE t_patch_stale_file (id UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                 patch_parts_version = '$version', min_bytes_for_full_part_storage = 0;
    "

    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_patch_stale_file SELECT number, 0 FROM numbers(1000)"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_patch_stale_file"

    ${CLICKHOUSE_CLIENT} --query "
        SET enable_lightweight_update = 1;
        UPDATE t_patch_stale_file SET v = 1 WHERE id < 400;
    "

    patch_part_path=$(${CLICKHOUSE_CLIENT} --query "
        SELECT any(path) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_patch_stale_file'
          AND active AND startsWith(partition_id, 'patch-')
    ")

    if [ -z "$patch_part_path" ]; then
        echo "FAIL: no patch part created for $version"
        continue
    fi

    printf '_block_number\n_block_offset\n' > "${patch_part_path}invalidated_system_columns.txt"

    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_patch_stale_file"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_patch_stale_file"

    echo -n "$version after reload: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(v = 1) FROM t_patch_stale_file"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_patch_stale_file"
    ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_patch_stale_file FINAL"

    echo -n "$version after optimize: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(v = 1) FROM t_patch_stale_file"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_patch_stale_file"
done
