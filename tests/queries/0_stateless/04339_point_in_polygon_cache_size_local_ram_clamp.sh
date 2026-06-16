#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local must clamp the `point_in_polygon_cache_size` server setting to the
# RAM-derived cache cap (physical_memory * cache_size_to_ram_max_ratio), the same way the
# server does. Otherwise a low-memory local run could apply the raw 256 MiB default and
# report that unclamped value in system.server_settings.
#
# The clamped value depends on the host's RAM, so the assertions are relationships rather
# than exact bytes: a generous ratio leaves the default untouched, while a tiny ratio clamps
# it below the default (but keeps it positive). The function must keep working either way.

GENEROUS_CONFIG=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04339_generous.XXXXXX.xml)
TINY_CONFIG=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04339_tiny.XXXXXX.xml)
trap 'rm -f "$GENEROUS_CONFIG" "$TINY_CONFIG"' EXIT

cat > "$GENEROUS_CONFIG" <<'EOF'
<clickhouse>
    <cache_size_to_ram_max_ratio>0.9</cache_size_to_ram_max_ratio>
</clickhouse>
EOF

cat > "$TINY_CONFIG" <<'EOF'
<clickhouse>
    <cache_size_to_ram_max_ratio>0.0000001</cache_size_to_ram_max_ratio>
</clickhouse>
EOF

echo "-- generous ram ratio: 256 MiB default applied unclamped --"
${CLICKHOUSE_LOCAL} --config-file "$GENEROUS_CONFIG" --query "
    SELECT value FROM system.server_settings WHERE name = 'point_in_polygon_cache_size'"

echo "-- tiny ram ratio: default clamped below it but still positive --"
${CLICKHOUSE_LOCAL} --config-file "$TINY_CONFIG" --query "
    SELECT toUInt64(value) < 268435456 AND toUInt64(value) > 0
    FROM system.server_settings WHERE name = 'point_in_polygon_cache_size'"

echo "-- pointInPolygon still works under the tiny cap --"
${CLICKHOUSE_LOCAL} --config-file "$TINY_CONFIG" --query "
    SELECT pointInPolygon((2., 2.), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)])"
