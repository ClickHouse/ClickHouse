#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verify that <print-memory-to-stderr> in a client config file activates the
# corresponding behaviour, and that an explicit CLI --memory-usage overrides it.
# This guards the !defaulted() check on --memory-usage in
# ClientBase::addOptionsToTheClientConfiguration: without it, the CLI default
# ('none') silently overwrites the value loaded from the config file.

config=$CLICKHOUSE_TMP/client_memory_usage_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config}"
}
trap cleanup EXIT

cat > "$config" <<'EOF'
<config>
    <print-memory-to-stderr>readable</print-memory-to-stderr>
</config>
EOF

echo "-- print-memory-to-stderr=readable from config, no CLI override (readable size on stderr)"
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1 >/dev/null | grep -c -E '^[0-9]+\.[0-9]+ (B|KiB|MiB|GiB)$'

echo "-- print-memory-to-stderr=readable from config, CLI --memory-usage none wins (nothing on stderr)"
$CLICKHOUSE_CLIENT --config "$config" --memory-usage none -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1 >/dev/null | grep -c -E '^[0-9]+\.[0-9]+ (B|KiB|MiB|GiB)$' || true

echo "-- print-memory-to-stderr=readable from config, CLI --memory-usage default wins (plain byte count)"
$CLICKHOUSE_CLIENT --config "$config" --memory-usage default -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1 >/dev/null | grep -c -E '^[0-9]+$'
