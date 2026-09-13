#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# The test inspects a local `MergeTree` data directory.
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS readonly_toggle_cleanup;
    CREATE TABLE readonly_toggle_cleanup (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS disk = 'default', temporary_directories_lifetime = 0,
        merge_tree_clear_old_temporary_directories_interval_seconds = 1,
        cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0;
    SYSTEM STOP CLEANUP readonly_toggle_cleanup;
"
table_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'readonly_toggle_cleanup'")
cleanup_dir="${table_path}/tmp_readonly_toggle_cleanup"
mkdir "$cleanup_dir"

${CLICKHOUSE_CLIENT} --multiquery --query "
    ALTER TABLE readonly_toggle_cleanup MODIFY SETTING table_readonly = 1;
    ALTER TABLE readonly_toggle_cleanup MODIFY SETTING table_readonly = 0;
"
test -d "$cleanup_dir"
echo 'Cleanup remains stopped'

${CLICKHOUSE_CLIENT} --query 'SYSTEM START CLEANUP readonly_toggle_cleanup'
timeout 60 bash -c 'while test -d "$1"; do sleep 0.1; done' bash "$cleanup_dir"
echo 'Cleanup resumes explicitly'
${CLICKHOUSE_CLIENT} --query 'DROP TABLE readonly_toggle_cleanup'
