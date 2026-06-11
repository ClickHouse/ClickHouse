#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="pr_data_paths_${CLICKHOUSE_DATABASE}"
user="pr_data_paths_user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS pr_data_paths SYNC"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE pr_data_paths (a Int32, b String) ORDER BY a
SETTINGS disk = disk(
    name = ${disk_name},
    type = object_storage,
    object_storage_type = local,
    metadata_type = plain_rewritable,
    path = 'disks/04326/${CLICKHOUSE_DATABASE}/')
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO pr_data_paths SELECT number, toString(number) FROM numbers(100)"

echo "-- at least one blob is reported for the disk"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.plain_rewritable_data_paths WHERE disk_name = '${disk_name}'"

echo "-- remote_path always ends with directory_remote_path/name"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.plain_rewritable_data_paths
WHERE disk_name = '${disk_name}' AND NOT endsWith(remote_path, concat(directory_remote_path, '/', name))"

echo "-- remote_path always starts with the common blob prefix"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.plain_rewritable_data_paths
WHERE disk_name = '${disk_name}' AND NOT startsWith(remote_path, common_prefix_for_blobs)"

echo "-- non-root local data paths are present"
${CLICKHOUSE_CLIENT} --query "
SELECT count() >= 1 FROM system.plain_rewritable_data_paths
WHERE disk_name = '${disk_name}' AND local_path != ''"

echo "-- a freshly written table has no ephemeral entries"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.plain_rewritable_data_paths
WHERE disk_name = '${disk_name}' AND is_ephemeral"

echo "-- a user with SELECT but without SHOW TABLES is denied"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} NOT IDENTIFIED"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.plain_rewritable_data_paths TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM system.plain_rewritable_data_paths" 2>&1 | grep -qF "ACCESS_DENIED" && echo "1" || echo "0"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE pr_data_paths SYNC"
