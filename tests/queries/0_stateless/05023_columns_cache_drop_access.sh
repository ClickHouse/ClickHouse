#!/usr/bin/env bash
# Tags: no-parallel
# The test drops the process-wide columns cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05023_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user}"

if $CLICKHOUSE_CLIENT --user "${user}" --query "SYSTEM DROP COLUMNS CACHE" > /dev/null 2>&1; then
    echo "unexpected success without privilege"
    exit 1
fi
echo "denied without privilege"

$CLICKHOUSE_CLIENT --query "GRANT SYSTEM DROP COLUMNS CACHE ON *.* TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" --query "SYSTEM DROP COLUMNS CACHE"
echo "succeeds with privilege"

$CLICKHOUSE_CLIENT --query "DROP USER ${user}"
