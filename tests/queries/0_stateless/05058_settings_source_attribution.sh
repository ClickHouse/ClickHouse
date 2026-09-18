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
    <merge_tree>
        <merge_max_block_size>1234</merge_max_block_size>
        <!-- Its default: an explicit assignment of the value a setting already has. -->
        <max_suspicious_broken_parts>100</max_suspicious_broken_parts>
        <!-- Rolled back by `compatibility` too, so that the two sources meet on one setting. -->
        <compute_exact_num_defaults_for_sparse_columns>0</compute_exact_num_defaults_for_sparse_columns>
        <index_granularity>4096</index_granularity>
    </merge_tree>
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

echo "-- a config section assigning the value a setting already has still counts"
# Recorded while the server-level baseline is built, which is the only place it can be seen: a
# comparison of values afterwards finds nothing to report.
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT name, value = \`default\` AS same_as_default, source FROM system.table_settings
WHERE table = 'mt' AND name = 'max_suspicious_broken_parts';"

echo "-- for MergeTree too, the definition wins over the config"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS merge_max_block_size = 4321;
SELECT name, value, source FROM system.table_settings
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

# Coverage rather than a regression guard for any one change: the two sources had simply never met in a
# single run, so nothing pinned which of them wins.
echo "-- the config section wins where it and the compatibility setting touch one setting"
# The two are applied in that order, and this is the only place they meet: `compute_exact_num_defaults_for_
# sparse_columns` is rolled back by 23.3 and then assigned by the config to the same value the roll-back gave
# it, which no comparison of values could tell from either source alone.
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --compatibility=23.3 -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT name, value, source FROM system.table_settings
WHERE table = 'mt' AND name IN ('compute_exact_num_defaults_for_sparse_columns', 'merge_max_block_size')
ORDER BY name;"

echo "-- an engine argument is not the config, even for a setting the config also sets"
# The old syntax passes `index_granularity` as the third engine argument, which assigns it after the
# server's baseline; the baseline's mark must not survive that.
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
SET allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE mt (d Date, a UInt64) ENGINE = MergeTree(d, a, 16384);
SELECT name, value, source FROM system.table_settings WHERE table = 'mt' AND name = 'index_granularity';"

echo "-- settings compatibility rolled back, against the current defaults"
$CLICKHOUSE_LOCAL --compatibility=23.3 -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT count() > 0 AS rolled_back, countIf(value = \`default\`) AS same_as_default
FROM system.table_settings WHERE table = 'mt' AND source = 'compatibility';"

echo "-- a session's compatibility does not decide what the server's defaults are reported to be"
# The `MergeTree` baseline is built once and shared by every session and by every table created afterwards,
# so it takes the compatibility of the server rather than of whoever asks first. Through `clickhouse-local`,
# where that distinction is observable: the process is fresh, so this session *is* the first to ask, and
# before the baseline took the global context's compatibility this printed the rolled-back count instead.
$CLICKHOUSE_LOCAL -q "
SET compatibility = '23.3';
SELECT countIf(value != \`default\`) FROM system.engine_settings WHERE engine_name = 'MergeTree';"

echo "-- and none of them when nothing sets anything"
$CLICKHOUSE_LOCAL -q "
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a;
SELECT countIf(source = 'config') + countIf(source = 'compatibility') FROM system.table_settings WHERE table = 'mt';"

rm -f "$CONFIG"
