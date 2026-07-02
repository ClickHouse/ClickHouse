#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE system.user_query_log FORMAT TSVRaw" | grep -F "CREATE VIEW system.user_query_log" >/dev/null
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE system.user_query_log FORMAT TSVRaw" | grep -F "SQL SECURITY NONE" >/dev/null
echo "show create ok"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        engine,
        create_table_query != '',
        position(create_table_query, 'SQL SECURITY NONE') > 0,
        position(as_select, 'currentUser()') > 0,
        position(as_select, 'initial_user') > 0,
        definer = ''
    FROM system.tables
    WHERE database = 'system' AND name = 'user_query_log'"
