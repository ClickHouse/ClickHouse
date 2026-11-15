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

cat > "$config" <<EOF
<config>
    <print-profile-events>banana</print-profile-events>
</config>
EOF
echo "-- non-boolean print-profile-events in config is rejected before the query runs"
$CLICKHOUSE_CLIENT --config "$config" -q "INSERT INTO t_04629 VALUES (1)" 2>&1 >/dev/null | grep -c -F "Invalid value 'banana' for the 'print-profile-events' configuration key"

# Boundary regression: each key must be validated with the same range as its eventual read
# site. These values parse as UInt64 but overflow the narrower consumers (`history_max_entries`
# is read as UInt, `suggestion_limit` as Int), so they must be rejected up front as well.
for pair in "history_max_entries 5000000000" "suggestion_limit 3000000000"
do
    key=${pair% *}
    value=${pair#* }
    cat > "$config" <<EOF
<config>
    <${key}>${value}</${key}>
</config>
EOF
    echo "-- out-of-range ${key} in config is rejected before the query runs"
    $CLICKHOUSE_CLIENT --config "$config" -q "INSERT INTO t_04629 VALUES (1)" 2>&1 >/dev/null | grep -c -F "Invalid value '${value}' for the '${key}' configuration key"
done

echo "-- a wide value is accepted where the read site is 64-bit"
cat > "$config" <<'EOF'
<config>
    <chime-threshold-seconds>5000000000</chime-threshold-seconds>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT 1"

echo "-- no rows were inserted"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04629"

echo "-- the empty <print-profile-events/> form means enabled"
# Under load the server may deliver the profile events in several packets, splitting the
# increment across multiple lines, so only check that the counter is printed at all.
cat > "$config" <<'EOF'
<config>
    <print-profile-events/>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" --max_block_size=65505 -q 'SELECT * FROM numbers(1e5) FORMAT Null' |& grep -q -F 'SelectedRows' && echo "SelectedRows printed"

echo "-- a valid numeric value is accepted"
cat > "$config" <<'EOF'
<config>
    <chime-threshold-seconds>5</chime-threshold-seconds>
</config>
EOF
$CLICKHOUSE_CLIENT --config "$config" -q "SELECT 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04629"
