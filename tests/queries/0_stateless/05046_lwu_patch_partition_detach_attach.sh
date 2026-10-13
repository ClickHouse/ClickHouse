#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# DETACH + ATTACH of a patch partition used to corrupt the patch part: the attach
# path wrote `invalidated_system_columns.txt` with `_block_number`/`_block_offset`
# for every attached part, but in patch parts these columns are the payload (row
# identities in the original parts). Reading the re-attached patch part then
# returned a null `_block_number` column and applying the patch crashed the server.

for version in v1 v2; do
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS t_patch_attach;
        CREATE TABLE t_patch_attach (id UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = '$version';
    "

    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_patch_attach SELECT number, 0 FROM numbers(1000)"

    ${CLICKHOUSE_CLIENT} --query "
        SET enable_lightweight_update = 1;
        UPDATE t_patch_attach SET v = 1 WHERE id < 400;
    "

    patch_partition_id=$(${CLICKHOUSE_CLIENT} --query "
        SELECT any(partition_id) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_patch_attach'
          AND active AND startsWith(partition_id, 'patch-')
    ")

    if [ -z "$patch_partition_id" ]; then
        echo "FAIL: no patch partition created for $version"
        continue
    fi

    echo -n "$version before detach: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(v = 1) FROM t_patch_attach"

    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_patch_attach DETACH PARTITION ID '$patch_partition_id'"

    echo -n "$version after detach: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(v = 1) FROM t_patch_attach"

    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_patch_attach ATTACH PARTITION ID '$patch_partition_id'"

    echo -n "$version after attach: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(v = 1) FROM t_patch_attach"

    ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_patch_attach FINAL"

    echo -n "$version after optimize: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(v = 1) FROM t_patch_attach"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_patch_attach"
done
