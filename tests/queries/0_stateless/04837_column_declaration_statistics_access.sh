#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_04837"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab (x UInt64) ENGINE = MergeTree ORDER BY x;

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER ADD COLUMN, ALTER MODIFY COLUMN ON $CLICKHOUSE_DATABASE.tab TO $user_name;
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

# A column-declaration STATISTICS in ADD COLUMN / MODIFY COLUMN adds or replaces statistics,
# so it must require the same access as the dedicated ADD STATISTICS / MODIFY STATISTICS commands.
check_access "ALTER TABLE tab ADD COLUMN y UInt64 STATISTICS(tdigest)"
check_access "ALTER TABLE tab MODIFY COLUMN x UInt64 STATISTICS(tdigest)"
check_access "ALTER TABLE tab MODIFY COLUMN x STATISTICS(tdigest) COMMENT 'text'"

# Without a STATISTICS declaration the column grants are enough.
check_access "ALTER TABLE tab ADD COLUMN y UInt64"
check_access "ALTER TABLE tab MODIFY COLUMN x UInt64 COMMENT 'text'"

$CLICKHOUSE_CLIENT -q "GRANT ALTER ADD STATISTICS, ALTER MODIFY STATISTICS ON $CLICKHOUSE_DATABASE.tab TO $user_name;"

check_access "ALTER TABLE tab ADD COLUMN z UInt64 STATISTICS(tdigest)"
check_access "ALTER TABLE tab MODIFY COLUMN x UInt64 STATISTICS(tdigest)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE tab;
    DROP USER $user_name;
"
