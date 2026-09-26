#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-local` loads the same client config surface as `clickhouse-client`
# (`LocalServer::initialize`), so malformed config-backed values must be rejected there
# as well, before any query can start (fail fast, not fail open).

config=$CLICKHOUSE_TMP/local_config_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config}"
}
trap cleanup EXIT

for key in chime-threshold-seconds profile-events-delay-ms history_max_entries suggestion_limit
do
    cat > "$config" <<EOF
<config>
    <${key}>banana</${key}>
</config>
EOF
    echo "-- non-numeric ${key} in config is rejected by clickhouse-local"
    $CLICKHOUSE_LOCAL --config-file "$config" -q "SELECT 1" 2>&1 >/dev/null | grep -c -F "Invalid value 'banana' for the '${key}' configuration key"
done

cat > "$config" <<'EOF'
<config>
    <print-profile-events>banana</print-profile-events>
</config>
EOF
echo "-- non-boolean print-profile-events in config is rejected by clickhouse-local"
$CLICKHOUSE_LOCAL --config-file "$config" -q "SELECT 1" 2>&1 >/dev/null | grep -c -F "Invalid value 'banana' for the 'print-profile-events' configuration key"

cat > "$config" <<'EOF'
<config>
    <print-memory-to-stderr>defualt</print-memory-to-stderr>
</config>
EOF
echo "-- a typo in print-memory-to-stderr in config is rejected by clickhouse-local"
$CLICKHOUSE_LOCAL --config-file "$config" -q "SELECT 1" 2>&1 >/dev/null | grep -c -F "Unknown memory-usage mode: defualt"

cat > "$config" <<'EOF'
<config>
    <chime-threshold-seconds>5</chime-threshold-seconds>
</config>
EOF
echo "-- a valid value is accepted by clickhouse-local"
$CLICKHOUSE_LOCAL --config-file "$config" -q "SELECT 1"
