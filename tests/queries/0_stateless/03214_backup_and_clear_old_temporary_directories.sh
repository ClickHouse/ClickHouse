#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# In this test we restore from "/tests/queries/0_stateless/backups/mt_250_parts.zip"
backup_name="$($CURDIR/helpers/install_predefined_backup.sh mt_250_parts.zip)"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS manyparts;
CREATE TABLE manyparts (x Int64) ENGINE=MergeTree ORDER BY tuple() SETTINGS merge_tree_clear_old_temporary_directories_interval_seconds=1, temporary_directories_lifetime=1;
"

# RESTORE must protect its temporary directories from removing.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE default.mt_250_parts AS manyparts FROM Disk('backups', '${backup_name}') SETTINGS allow_different_table_def=true" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} -m --query "
SELECT count(), sum(x) FROM manyparts;
DROP TABLE manyparts;
"
