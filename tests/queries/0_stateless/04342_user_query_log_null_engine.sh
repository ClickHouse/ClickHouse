#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

test_dir=$(mktemp -d "${CLICKHOUSE_TMP%/}/${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
test_dir=$(cd "${test_dir}" && pwd)

server_pid=
cleanup()
{
    local status=$?
    trap - EXIT
    if [[ -n "${server_pid}" ]]
    then
        kill "${server_pid}" >/dev/null 2>&1 || true
        wait "${server_pid}" >/dev/null 2>&1 || true
    fi
    rm -rf "${test_dir}"
    exit "${status}"
}
trap cleanup EXIT

cat > "${test_dir}/users.xml" <<EOF
<clickhouse>
    <profiles>
        <default>
            <log_queries>1</log_queries>
        </default>
    </profiles>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
EOF

# Ask the OS for a currently unused TCP port instead of deriving one from the
# shell PID: a fixed port range clashes with the many servers (and ephemeral
# sockets) running in parallel during the test run, which made this test flaky
# with "Address already in use" on startup.
free_tcp_port()
{
    python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
"
}

# The test only connects over the native TCP protocol, so the server listens
# on a single port. There is still a tiny window between picking the free port
# and the server binding it, so retry with a fresh port if the server aborts.
tcp_port=
start_server()
{
    tcp_port=$(free_tcp_port)

    cat > "${test_dir}/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>warning</level>
        <log>${test_dir}/server.log</log>
        <errorlog>${test_dir}/server.err.log</errorlog>
        <size>100M</size>
        <count>1</count>
    </logger>

    <listen_host>127.0.0.1</listen_host>
    <tcp_port>${tcp_port}</tcp_port>

    <path>${test_dir}/data/</path>
    <tmp_path>${test_dir}/tmp/</tmp_path>
    <user_files_path>${test_dir}/user_files/</user_files_path>
    <format_schema_path>${test_dir}/format_schemas/</format_schema_path>
    <custom_cached_disks_base_directory>${test_dir}/caches/</custom_cached_disks_base_directory>

    <user_directories>
        <users_xml>
            <path>${test_dir}/users.xml</path>
        </users_xml>
        <local_directory>
            <path>${test_dir}/access/</path>
        </local_directory>
    </user_directories>

    <query_log>
        <database>system</database>
        <table>custom_query_log</table>
        <engine>ENGINE = Null</engine>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <enable_user_query_log>true</enable_user_query_log>
    </query_log>
</clickhouse>
EOF

    ${CLICKHOUSE_BINARY} server --config-file="${test_dir}/config.xml" > "${test_dir}/server.stdout.log" 2>&1 &
    server_pid=$!

    for _ in {1..120}
    do
        if ${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "SELECT 1" >/dev/null 2>&1
        then
            return 0
        fi
        # The server exits when it cannot bind the port, so do not keep waiting.
        if ! kill -0 "${server_pid}" >/dev/null 2>&1
        then
            return 1
        fi
        sleep 0.25
    done
    return 1
}

ready=0
for _ in {1..5}
do
    if start_server
    then
        ready=1
        break
    fi
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
    server_pid=
done

if [[ "${ready}" == 0 ]]
then
    cat "${test_dir}/server.log" >&2 || true
    cat "${test_dir}/server.err.log" >&2 || true
    exit 1
fi

${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "SELECT 42 FORMAT Null"
${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "
    SELECT engine
    FROM system.tables
    WHERE database = 'system' AND name = 'custom_query_log'"

${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "
    SELECT comment
    FROM system.tables
    WHERE database = 'system' AND name = 'user_query_log'"

${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "SELECT count() FROM system.user_query_log"
