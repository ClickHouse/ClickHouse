#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CONFIG="${CLICKHOUSE_TMP}/config.xml"

cat <<EOF >"${CONFIG}"
<clickhouse>
    <show_addresses_in_stack_traces>false</show_addresses_in_stack_traces>
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
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --query "SELECT throwIf(1)" --stacktrace --config-file "${CONFIG}" 2>&1 | grep -c -F '@ 0x'

sed 's/<show_addresses_in_stack_traces>false/<show_addresses_in_stack_traces>true/' "${CONFIG}" >"$CONFIG.1"

${CLICKHOUSE_LOCAL} --query "SELECT throwIf(1)" --stacktrace --config-file "${CONFIG}.1" 2>&1 | grep -c -F '@ 0x' | grep -c -v '^0$'

rm "${CONFIG}" "$CONFIG.1"
