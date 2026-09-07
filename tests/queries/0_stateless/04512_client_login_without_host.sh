#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the client in the fast test build is compiled without OAuth support, so --login does nothing there.

# `clickhouse client --login` without an explicit --host used to abort with a libc++ hardening
# assertion ("front() called on an empty vector") instead of reporting a proper error.
# When no --host is passed on the command line, the host must be taken from the configuration
# (config file, --connection, environment) with a fallback to localhost.
# https://github.com/ClickHouse/ClickHouse/issues/103603

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG=$CLICKHOUSE_TMP/04512_client_login.xml
cat > "$CONFIG" <<EOL
<clickhouse>
    <connections_credentials>
        <connection>
            <name>test_04512_connection</name>
            <hostname>host-from-connection.invalid</hostname>
        </connection>
    </connections_credentials>
</clickhouse>
EOL

# No host anywhere: must not abort and must fall back to localhost.
env -u CLICKHOUSE_HOST "$CLICKHOUSE_CLIENT_BINARY" --config "$CONFIG" --login 2>&1 | grep -o -m1 "Could not retrieve authentication endpoints for host 'localhost'"

# Host from the environment.
env CLICKHOUSE_HOST=host-from-env.invalid "$CLICKHOUSE_CLIENT_BINARY" --config "$CONFIG" --login 2>&1 | grep -o -m1 "Could not retrieve authentication endpoints for host 'host-from-env.invalid'"

# Host from a named connection in the config file.
env -u CLICKHOUSE_HOST "$CLICKHOUSE_CLIENT_BINARY" --config "$CONFIG" --connection test_04512_connection --login 2>&1 | grep -o -m1 "Could not retrieve authentication endpoints for host 'host-from-connection.invalid'"

rm "$CONFIG"
