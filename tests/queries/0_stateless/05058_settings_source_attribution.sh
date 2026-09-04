#!/usr/bin/env bash
# `system.table_settings` reports where each value came from. Two of those sources are set outside
# any query - a server config section, and the `compatibility` setting - and both are read once into
# a server-wide instance, so a per-query SETTINGS clause cannot exercise them. These run
# clickhouse-local with the config and the flag instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG="${CLICKHOUSE_TMP}/settings_source_attribution_config.xml"
cat > "$CONFIG" <<'EOF'
<clickhouse>
    <merge_tree><merge_max_block_size>1234</merge_max_block_size></merge_tree>
    <distributed><bytes_to_throw_insert>1000000</bytes_to_throw_insert></distributed>
    <remote_servers>
        <c1><shard><replica><host>localhost</host><port>9000</port></replica></shard></c1>
    </remote_servers>
</clickhouse>
EOF

echo "-- a MergeTree setting the config sets"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT name, value, \`default\`, source FROM system.table_settings
WHERE table = 'mt' AND name = 'merge_max_block_size';"

echo "-- a Distributed setting the config sets"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
CREATE TABLE src (a UInt64) ENGINE = Memory;
CREATE TABLE d AS src ENGINE = Distributed('c1', currentDatabase(), 'src');
SELECT name, value, \`default\`, source FROM system.table_settings
WHERE table = 'd' AND name = 'bytes_to_throw_insert';"

echo "-- the definition wins over the config, and is recognised through an alias"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
CREATE TABLE src (a UInt64) ENGINE = Memory;
CREATE TABLE d AS src ENGINE = Distributed('c1', currentDatabase(), 'src')
    SETTINGS bytes_to_throw_insert = 7, monitor_batch_inserts = 1;
SELECT name, value, source FROM system.table_settings
WHERE table = 'd' AND name IN ('bytes_to_throw_insert', 'background_insert_batch') ORDER BY name;"

echo "-- settings compatibility rolled back, against the current defaults"
$CLICKHOUSE_LOCAL --compatibility=23.3 -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT count() > 0 AS rolled_back, countIf(value = \`default\`) AS same_as_default
FROM system.table_settings WHERE table = 'mt' AND source = 'compatibility';"

echo "-- and none of them when nothing sets anything"
$CLICKHOUSE_LOCAL -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT countIf(source = 'config') + countIf(source = 'compatibility') FROM system.table_settings WHERE table = 'mt';"

rm -f "$CONFIG"
