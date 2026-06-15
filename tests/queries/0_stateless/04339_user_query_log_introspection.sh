#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE system.user_query_log FORMAT TSVRaw" | grep -F "CREATE VIEW system.user_query_log" >/dev/null
echo "show create ok"

${CLICKHOUSE_CLIENT} --query "
    SELECT engine, create_table_query != '', position(as_select, 'currentUser()') > 0
    FROM system.tables
    WHERE database = 'system' AND name = 'user_query_log'"
