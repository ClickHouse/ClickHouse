#!/usr/bin/env bash
# Tags: no-parallel
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SAFE_DIR="${CUR_DIR}/${CLICKHOUSE_DATABASE}_02360_local"
mkdir -p "${SAFE_DIR}"

echo "<clickhouse>
    <logger>
        <level>trace</level>
        <console>true</console>
    </logger>

    <tcp_port>${CLICKHOUSE_PORT_TCP}</tcp_port>

    <path>${SAFE_DIR}</path>

    <mark_cache_size>0</mark_cache_size>
    <user_directories>
        <users_xml>
            <!-- Path to configuration file with predefined users. -->
            <path>users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>" > $SAFE_DIR/config.xml

echo        "<clickhouse>
            <profiles>
                <default></default>
            </profiles>
            <users>
                <default>
                    <password></password>
                    <networks>
                        <ip>::/0</ip>
                    </networks>
                    <profile>default</profile>
                    <quota>default</quota>
                </default>
            </users>
            <quotas>
                <default></default>
            </quotas>
        </clickhouse>" > $SAFE_DIR/users.xml

local_opts=(
    "--config-file=$SAFE_DIR/config.xml"
    "--send_logs_level=none")

${CLICKHOUSE_LOCAL} "${local_opts[@]}" --query 'Select 1' |& grep -v -e 'Processing configuration file'

rm -rf "${SAFE_DIR}"
