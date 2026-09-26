#!/usr/bin/env bash
#
# A `Distributed` table records which of its settings the `<distributed>` config section assigned - including
# one the section sets to its compiled-in default, which no comparison of values could tell from an untouched
# setting. Here `flush_on_detach` is configured as `1`, its default, and is still reported as `config`; a setting
# the section also names but the table's own `SETTINGS` clause restates is `definition`; one the section does not
# name is `default`.
#
# `clickhouse-local` with its own config file, because the server's config cannot be changed from a test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/05231_config.xml"

cat > "${CONFIG_FILE}" <<'EOF'
<clickhouse>
    <distributed>
        <flush_on_detach>1</flush_on_detach>
        <bytes_to_delay_insert>123456</bytes_to_delay_insert>
    </distributed>
    <remote_servers>
        <c><shard><replica><host>127.0.0.1</host><port>9000</port></replica></shard></c>
    </remote_servers>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --query "
CREATE TABLE src (x UInt8) ENGINE = Memory;
CREATE TABLE d (x UInt8) ENGINE = Distributed(c, default, src) SETTINGS bytes_to_delay_insert = 654321;
SELECT name, value, changed, source FROM system.table_settings
WHERE table = 'd' AND name IN ('flush_on_detach', 'bytes_to_delay_insert', 'max_delay_to_insert')
ORDER BY name"

rm "${CONFIG_FILE}"
