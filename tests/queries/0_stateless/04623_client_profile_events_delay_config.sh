#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verify that <profile-events-delay-ms> in a client config file takes effect when
# the CLI flag is omitted, and that an explicit CLI --profile-events-delay-ms
# overrides it. This guards the !defaulted() check on --profile-events-delay-ms in
# ClientBase::addOptionsToTheClientConfiguration and the config-layer read of
# profile_events.delay_ms: without them, the CLI default (0) silently overwrites
# the value loaded from the config file.
#
# With delay 18446744073709551615 (-1, "print only totals") the client never
# flushes ProfileEvents mid-query and prints a single aggregated block at the end,
# so each event name appears exactly once. With the buggy effective delay of 0,
# every incoming packet is printed immediately, and a multi-second query produces
# several SleepFunctionCalls lines.

config_totals=$CLICKHOUSE_TMP/client_profile_events_delay_totals_${CLICKHOUSE_DATABASE}.xml
config_zero=$CLICKHOUSE_TMP/client_profile_events_delay_zero_${CLICKHOUSE_DATABASE}.xml

function cleanup()
{
    rm -f "${config_totals}" "${config_zero}"
}
trap cleanup EXIT

cat > "$config_totals" <<'EOF'
<config>
    <profile-events-delay-ms>18446744073709551615</profile-events-delay-ms>
</config>
EOF

cat > "$config_zero" <<'EOF'
<config>
    <profile-events-delay-ms>0</profile-events-delay-ms>
</config>
EOF

echo "-- delay -1 from config, CLI omitted: totals only (one SleepFunctionCalls line)"
count=$($CLICKHOUSE_CLIENT --config "$config_totals" --max_block_size 1 --print-profile-events -q 'SELECT sleep(0.2) FROM numbers(10) FORMAT Null' |& grep -c 'SleepFunctionCalls')
test "$count" -eq 1 && echo OK || echo "FAIL ($count)"

echo "-- delay -1 from config, CLI omitted: totals are aggregated across the whole query"
$CLICKHOUSE_CLIENT --config "$config_totals" --max_block_size=65505 --print-profile-events -q 'SELECT * FROM numbers(1e5) FORMAT Null' |& grep -F -o '[ 0 ] SelectedRows: 100000 (increment)'

echo "-- delay 0 from config, explicit CLI --profile-events-delay-ms=-1 wins: totals only"
count=$($CLICKHOUSE_CLIENT --config "$config_zero" --profile-events-delay-ms=-1 --max_block_size 1 --print-profile-events -q 'SELECT sleep(0.2) FROM numbers(10) FORMAT Null' |& grep -c 'SleepFunctionCalls')
test "$count" -eq 1 && echo OK || echo "FAIL ($count)"
