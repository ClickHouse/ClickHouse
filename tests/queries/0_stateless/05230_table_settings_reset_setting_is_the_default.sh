#!/usr/bin/env bash
#
# A `MergeTree` table records which of its settings the `<merge_tree>` config section assigned. When the
# table's `SETTINGS` clause names `disk`, the engine resets `storage_policy` to its default, and a setting
# back at its default is reported as `default` - not as the config section it had come from before the reset.
# The other config-assigned setting is still reported as `config`.
#
# `clickhouse-local` with its own config file, because the server's config cannot be changed from a test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/05230_config.xml"

cat > "${CONFIG_FILE}" <<EOF
<clickhouse>
    <merge_tree>
        <storage_policy>default</storage_policy>
        <parts_to_throw_insert>1234</parts_to_throw_insert>
    </merge_tree>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --query "
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = 'default';
SELECT name, value, changed, source FROM system.table_settings
WHERE table = 't' AND name IN ('storage_policy', 'parts_to_throw_insert', 'disk')
ORDER BY name"

rm "${CONFIG_FILE}"
