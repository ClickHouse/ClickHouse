#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="03000_traverse_shadow_system_data_path_table"
BACKUP="03000_traverse_shadow_system_data_path_backup"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${TABLE} (
    id Int64,
    data String
) ENGINE=MergeTree()
ORDER BY id
SETTINGS storage_policy='s3_cache';"

${CLICKHOUSE_CLIENT} --query="INSERT INTO ${TABLE} VALUES (0, 'data');"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.remote_data_paths WHERE disk_name = 's3_cache'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE ${TABLE} FREEZE WITH NAME '${BACKUP}';"
${CLICKHOUSE_CLIENT} --query="DROP TABLE ${TABLE} SYNC;"

${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0
    FROM system.remote_data_paths 
    WHERE disk_name = 's3_cache' AND local_path LIKE '%shadow/${BACKUP}%'
    SETTINGS traverse_shadow_remote_data_paths=1;"
${CLICKHOUSE_CLIENT} --query "SYSTEM UNFREEZE WITH NAME '${BACKUP}';" &>/dev/null || true
