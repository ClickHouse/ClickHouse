#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Numeric client config keys are read lazily at their use sites, so they are validated
# once right after the client config file is loaded (`ClientBase::validateClientConfiguration`).
# A non-numeric value must be rejected before any query is sent (fail fast, not fail open).

config=$CLICKHOUSE_TMP/client_numeric_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04629 (x UInt8) ENGINE = MergeTree ORDER BY x"

for key in chime-threshold-seconds profile-events-delay-ms history_max_entries suggestion_limit
do
    cat > "$config" <<EOF
<config>
    <${key}>banana</${key}>
</config>
EOF
    echo "-- non-numeric ${key} in config is rejected before the query runs"
    $CLICKHOUSE_CLIENT --config "$config" -q "INSERT INTO t_04629 VALUES (1)" 2>&1 >/dev/null | grep -c -F "Invalid value 'banana' for the '${key}' configuration key"
done

echo "-- no rows were inserted"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04629"

echo "-- a valid numeric value is accepted"
cat > "$config" <<'EOF'
<config>
    <chime-threshold-seconds>5</chime-threshold-seconds>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04629"
