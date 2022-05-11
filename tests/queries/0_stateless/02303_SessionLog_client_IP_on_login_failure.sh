#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: relies on session_log entries, could break if there multiple ones added simultaneously

########################################################################################################
#
# Verify that if user fails to authenticate on server,
# then failed login entry in session_log has non-zero client IP address and port.
#
########################################################################################################

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# To make test is stable if executed multiple times
readonly USERNAME="02303_sessionLog_client_IP_on_login_failure_$(cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32)"

$CLICKHOUSE_CLIENT -u "${USERNAME}" -q "SELECT 1;" 2>/dev/null ||:

$CLICKHOUSE_CLIENT -nm <<EOF
    SYSTEM FLUSH LOGS;
    SELECT
        if(client_address != toIPv6('::ffff:0.0.0.0'), 'non-empty IPv6 address', toString(client_address)),
        if(client_port != 0, 'Non-empty port', toString(client_address))
    FROM system.session_log
    WHERE user == '${USERNAME}';
EOF
