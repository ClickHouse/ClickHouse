#!/usr/bin/env bash
# Tags: no-parallel
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "<clickhouse>
    <logger>
        <level>trace</level>
        <console>true</console>
    </logger>

    <tcp_port>9000</tcp_port>

    <path>./</path>

    <mark_cache_size>0</mark_cache_size>
    <user_directories>
        <users_xml>
            <!-- Path to configuration file with predefined users. -->
            <path>users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>" > $CUR_DIR/config.xml

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
        </clickhouse>" > $CUR_DIR/users.xml

local_opts=(
    "--config-file=$CUR_DIR/config.xml"
    "--send_logs_level=none")

${CLICKHOUSE_LOCAL} "${local_opts[@]}" --query 'Select 1' |& grep -v -e 'Processing configuration file'

rm -rf $CUR_DIR/users.xml
rm -rf $CUR_DIR/config.xml
