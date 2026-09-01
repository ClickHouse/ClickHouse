#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_05055"

trap '$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user_name"' EXIT

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;
CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
"

function run_as_user()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    if echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

# `getServerSetting`, `getMergeTreeSetting` and `getMacro` return the same values as `system.server_settings`,
# `system.merge_tree_settings` and `system.macros`, so they require the same grant. The user below has no grants.
run_as_user "SELECT getServerSetting('mark_cache_policy')"
run_as_user "SELECT getMergeTreeSetting('index_granularity')"
run_as_user "SELECT getMacro('test')"

# The settings of the current session are not privileged.
run_as_user "SELECT toString(getSetting('max_block_size')) = (SELECT value FROM system.settings WHERE name = 'max_block_size')"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.server_settings TO $user_name"

# The value must be the same one the system table reports.
run_as_user "SELECT getServerSetting('mark_cache_policy') = (SELECT value FROM system.server_settings WHERE name = 'mark_cache_policy')"
run_as_user "SELECT getMergeTreeSetting('index_granularity')"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.merge_tree_settings TO $user_name"

run_as_user "SELECT toString(getMergeTreeSetting('index_granularity')) = (SELECT value FROM system.merge_tree_settings WHERE name = 'index_granularity')"
run_as_user "SELECT getMacro('test')"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.macros TO $user_name"

run_as_user "SELECT getMacro('test') = (SELECT substitution FROM system.macros WHERE macro = 'test')"
