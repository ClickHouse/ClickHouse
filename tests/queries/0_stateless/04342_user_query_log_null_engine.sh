#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

port_base=$((20000 + $$ % 20000))
tcp_port=${port_base}
http_port=$((port_base + 1))
mysql_port=$((port_base + 2))
postgresql_port=$((port_base + 3))
interserver_http_port=$((port_base + 4))

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
    <http_port>${http_port}</http_port>
    <tcp_port>${tcp_port}</tcp_port>
    <mysql_port>${mysql_port}</mysql_port>
    <postgresql_port>${postgresql_port}</postgresql_port>
    <interserver_http_port>${interserver_http_port}</interserver_http_port>

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

ready=0
for _ in {1..120}
do
    if ${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "${tcp_port}" --query "SELECT 1" >/dev/null 2>&1
    then
        ready=1
        break
    fi
    sleep 0.25
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
