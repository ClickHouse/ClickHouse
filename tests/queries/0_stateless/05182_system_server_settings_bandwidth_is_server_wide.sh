#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `*_bandwidth_for_server` limit is server-wide, so the value reported for it must not depend on the
# session or the user that reads it.
rows="SELECT name, value FROM system.server_settings WHERE endsWith(name, 'bandwidth_for_server') ORDER BY name"

# The per-query limits are zeroed for the reference reading and for the dedicated user below: a non-zero
# per-query limit takes precedence over the per-user one, so leaving the profile's value in place would
# hide the per-user comparison.
no_limits="max_local_read_bandwidth = 0, max_local_write_bandwidth = 0,
    max_remote_read_network_bandwidth = 0, max_remote_write_network_bandwidth = 0"

server_wide=$($CLICKHOUSE_CLIENT -q "$rows SETTINGS $no_limits")

with_query_limits=$($CLICKHOUSE_CLIENT -q "$rows SETTINGS
    max_local_read_bandwidth = 1000001, max_local_write_bandwidth = 1000002,
    max_remote_read_network_bandwidth = 1000003, max_remote_write_network_bandwidth = 1000004,
    max_backup_bandwidth = 1000005")

if [ "$server_wide" = "$with_query_limits" ]; then
    echo "per_query_limits_excluded 1"
else
    echo "per_query_limits_excluded 0"
    diff <(echo "$server_wide") <(echo "$with_query_limits")
fi

user="user_${CLICKHOUSE_DATABASE}"
trap '$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"' EXIT

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password' SETTINGS max_network_bandwidth_for_user = 1234567;
GRANT SELECT ON system.server_settings TO $user;"

as_user=$($CLICKHOUSE_CLIENT --user "$user" --password "password" -q "$rows SETTINGS $no_limits")

if [ "$server_wide" = "$as_user" ]; then
    echo "per_user_limit_excluded 1"
else
    echo "per_user_limit_excluded 0"
    diff <(echo "$server_wide") <(echo "$as_user")
fi

# `getServerSetting` documents that it returns what this table returns.
agree=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('max_local_read_bandwidth_for_server')
    = toUInt64((SELECT value FROM system.server_settings WHERE name = 'max_local_read_bandwidth_for_server'))
    SETTINGS max_local_read_bandwidth = 1000006")
echo "readers_agree $agree"
