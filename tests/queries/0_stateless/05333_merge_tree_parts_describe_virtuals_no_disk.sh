#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

# `DESCRIBE` of `mergeTreeParts` never creates the disk, also when it lists the virtual columns
# (`describe_include_virtual_columns = 1`): they are known without executing the table function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_PATH="${CLICKHOUSE_DISKS_FILES}/mtp_describe_virtuals_${CLICKHOUSE_DATABASE}/"
rm -rf "${DISK_PATH}"

${CLICKHOUSE_CLIENT} --query "
    DESCRIBE mergeTreeParts(
        structure('x UInt8'),
        parts(),
        disk(type = local, path = '${DISK_PATH}'),
        table_settings(index_granularity_bytes = 10485760))
    SETTINGS describe_include_virtual_columns = 1
    FORMAT TSV" | cut -f1,2,8

test -d "${DISK_PATH}" && echo "the disk was created" || echo "no directory"
