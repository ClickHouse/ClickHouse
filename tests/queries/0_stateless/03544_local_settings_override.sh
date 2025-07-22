#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel - spawning bunch of processes with sanitizers can use significant amount of memory

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

local_path="${CUR_DIR:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
mkdir -p "$local_path"

cat > "$local_path/config.xml" <<EOL
<clickhouse>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>
EOL

cat > "$local_path/users.xml" <<EOL
<clickhouse>
    <profiles>
        <default>
          <max_threads>1</max_threads>
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
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
EOL
$CLICKHOUSE_LOCAL --path "$local_path" --config "$local_path/config.xml" -q "SELECT getSetting('max_threads')"
$CLICKHOUSE_LOCAL --path "$local_path" --config "$local_path/config.xml" --max_threads 10 -q "SELECT getSetting('max_threads')"

rm -fr "${local_path:?}"
