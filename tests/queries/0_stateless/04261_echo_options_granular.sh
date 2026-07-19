#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verify that <echo_formatted/> and <echo_query_id/> in a config file activate
# the corresponding behaviour, and that an explicit CLI flag overrides the config.
# This guards the echo_formatted / echo_query_id → echo-formatted / echo-query-id
# remapping in Client::processConfig against initialization-order regressions.

config=$CLICKHOUSE_TMP/echo_options_granular_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config}"
}
trap cleanup EXIT

# --- echo_formatted loaded from config ---

cat > "$config" <<'EOF'
<config>
    <echo_formatted>true</echo_formatted>
</config>
EOF

echo "-- echo_formatted=true from config, no CLI override (blank lines surround the formatted query)"
$CLICKHOUSE_CLIENT --config "$config" --echo -q "SELECT 1"

echo "-- echo_formatted=true from config, CLI --echo-formatted=false wins (no surrounding blank lines)"
$CLICKHOUSE_CLIENT --config "$config" --echo --echo-formatted=false -q "SELECT 1"

# --- echo_query_id loaded from config ---

cat > "$config" <<'EOF'
<config>
    <echo_query_id>true</echo_query_id>
</config>
EOF

echo "-- echo_query_id=true from config, no CLI override (query ID line present)"
$CLICKHOUSE_CLIENT --config "$config" --query-id test-query-123 -q "SELECT 1"

echo "-- echo_query_id=true from config, CLI --echo-query-id=false wins (no query ID line)"
$CLICKHOUSE_CLIENT --config "$config" --echo-query-id=false -q "SELECT 1"
