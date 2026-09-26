#!/usr/bin/env bash
#
# A `MergeTree` table records which of its settings the `<merge_tree>` config section assigned. When the
# table's `SETTINGS` clause names `disk`, the engine resets `storage_policy` to its default, which clears the
# changed bit, and the setting is reported as `default` - not as the config section it had come from before
# the reset. The section's value here equals the compiled-in default on purpose: a table that does not name
# `disk` still reports it as `config`, so the reset, not the value, is what makes the difference. The section's
# other key is `config` for both tables.
#
# `clickhouse-local` with its own config file, because the server's config cannot be changed from a test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/05230_config.xml"

cat > "${CONFIG_FILE}" <<'EOF'
<clickhouse>
    <merge_tree>
        <storage_policy>default</storage_policy>
        <parts_to_throw_insert>1234</parts_to_throw_insert>
    </merge_tree>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --query "
CREATE TABLE with_disk (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = 'default';
CREATE TABLE without_disk (x UInt8) ENGINE = MergeTree ORDER BY x;
SELECT table, name, value, changed, source FROM system.table_settings
WHERE table IN ('with_disk', 'without_disk') AND name IN ('storage_policy', 'parts_to_throw_insert', 'disk')
ORDER BY table, name"

rm "${CONFIG_FILE}"
