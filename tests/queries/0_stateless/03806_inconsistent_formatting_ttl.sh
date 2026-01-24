#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --query "
ALTER TABLE \`t340\` (MODIFY TTL (('-999:59:59'::Time) AS \`a0\`)) SETTINGS ignore_cold_parts_seconds = 60, hdfs_create_new_file_on_insert = 0, dictionary_validate_primary_key_type = 1, min_bytes_to_use_direct_io = 3512074, load_marks_asynchronously = 0;
" | $CLICKHOUSE_FORMAT
