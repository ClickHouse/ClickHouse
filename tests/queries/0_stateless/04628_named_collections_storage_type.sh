#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04628_config.XXXXXX.xml)
trap 'rm -f "$CONFIG"' EXIT

cat > "$CONFIG" <<'EOF'
<clickhouse>
    <named_collections_storage>
        <type>local</type>
    </named_collections_storage>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG" --query "
    SELECT name, value, default, changed, type, changeable_without_restart
    FROM system.server_settings
    WHERE name = 'named_collections_storage.type'"

${CLICKHOUSE_LOCAL} --config-file "$CONFIG" --query \
    "SELECT getServerSetting('named_collections_storage_type')"
