#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local must clamp the `point_in_polygon_cache_size` server setting to the
# RAM-derived cache cap (physical_memory * cache_size_to_ram_max_ratio), the same way the
# server does. Otherwise a low-memory local run could apply a raw oversized value and report
# it (unclamped) in system.server_settings.
#
# The clamped value depends on the host's RAM, so the assertions are deliberately RAM-robust:
#  - a small configured size under a generous ratio stays below the cap, so it is applied as-is;
#  - a large configured size under a tiny ratio is above the cap, so it is clamped to a value
#    that is below the configured size but still positive.
# Both hold for any host with roughly 10 MiB .. 10 PiB of memory, i.e. every real machine.

UNCLAMPED_CONFIG=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04339_unclamped.XXXXXX.xml)
CLAMPED_CONFIG=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04339_clamped.XXXXXX.xml)
trap 'rm -f "$UNCLAMPED_CONFIG" "$CLAMPED_CONFIG"' EXIT

cat > "$UNCLAMPED_CONFIG" <<'EOF'
<clickhouse>
    <point_in_polygon_cache_size>1048576</point_in_polygon_cache_size>
    <cache_size_to_ram_max_ratio>0.9</cache_size_to_ram_max_ratio>
</clickhouse>
EOF

cat > "$CLAMPED_CONFIG" <<'EOF'
<clickhouse>
    <point_in_polygon_cache_size>1073741824</point_in_polygon_cache_size>
    <cache_size_to_ram_max_ratio>0.0000001</cache_size_to_ram_max_ratio>
</clickhouse>
EOF

echo "-- 1 MiB configured, generous ram ratio: applied unclamped --"
${CLICKHOUSE_LOCAL} --config-file "$UNCLAMPED_CONFIG" --query "
    SELECT value FROM system.server_settings WHERE name = 'point_in_polygon_cache_size'"

echo "-- 1 GiB configured, tiny ram ratio: clamped below it but still positive --"
${CLICKHOUSE_LOCAL} --config-file "$CLAMPED_CONFIG" --query "
    SELECT toUInt64(value) < 1073741824 AND toUInt64(value) > 0
    FROM system.server_settings WHERE name = 'point_in_polygon_cache_size'"

echo "-- pointInPolygon still works under the tiny cap --"
${CLICKHOUSE_LOCAL} --config-file "$CLAMPED_CONFIG" --query "
    SELECT pointInPolygon((2., 2.), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)])"
