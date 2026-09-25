#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `SYSTEM RELOAD CONFIG` is global server state, and the test
# temporarily changes a server-wide memory setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `additional_memory_tracking_per_thread` is changeable without a restart. The value
# `system.server_settings` reports must be the live effective one - read back from the
# atomic the pipeline executor consults - and not the startup value: two consecutive
# reloads with different values must be reported as two different values.
#
# The clamp of an oversized value is checked in the second part of the test with a
# private `clickhouse-local`: while such a reservation is live, every pipeline worker
# reserves the whole physical memory, so it must not be applied to the shared server.

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/${CLICKHOUSE_TEST_NAME}.xml

function reload_config()
{
    # In case of listen_try we can have 'Address already in use'
    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'
}

function show_setting()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT value, changeable_without_restart
        FROM system.server_settings
        WHERE name = 'additional_memory_tracking_per_thread'
        SETTINGS max_threads = 1"
}

function set_setting()
{
    cat > "$config_path" <<XML
<clickhouse>
    <additional_memory_tracking_per_thread>$1</additional_memory_tracking_per_thread>
</clickhouse>
XML
    reload_config
}

function restore_config()
{
    rm -f "$config_path"
    reload_config
}
trap restore_config EXIT

baseline=$(show_setting)
echo "$baseline" | cut -f2

# 1. An explicit value takes effect on reload and is reported as-is.
set_setting 8388608
show_setting

# 2. A second reload with a different value must be reported as the new value: a
#    regression that keeps reporting the startup or the previously loaded value fails
#    here.
set_setting 16777216
show_setting

# 3. Removing the override restores the startup value on the next reload.
trap - EXIT
restore_config
if [ "$(show_setting)" == "$baseline" ]
then
    echo "restored"
else
    echo "not restored: baseline '$baseline', now '$(show_setting)'"
fi

# 4. An oversized value (`UInt64` max, far above `INT64_MAX`) is clamped to the physical
#    server memory, and it is the clamped value that is published as the live one. The
#    exact physical memory is not asserted because it depends on the cgroup of the
#    server; it is only required to be finite, positive, larger than the values used
#    above and not larger than the total memory of the machine.
#
#    The clamped reservation is at least the whole physical memory, which is above any
#    server memory limit derived from the default ratio, so the query would be rejected
#    by its own reservation. `max_server_memory_usage_to_ram_ratio` is therefore raised
#    far above 1 (an explicit `max_server_memory_usage` cannot be used: it is capped by
#    that ratio).
os_memory_total=$($CLICKHOUSE_CLIENT --query "
    SELECT toUInt64(value)
    FROM system.asynchronous_metrics
    WHERE metric = 'OSMemoryTotal'
    SETTINGS max_threads = 1")
# The asynchronous metric is only used as a sanity bound; if it is not collected yet,
# fall back to the largest possible value.
if [ -z "$os_memory_total" ] || [ "$os_memory_total" -le 0 ]
then
    os_memory_total=9223372036854775807
fi

local_config=$(mktemp -p "${CLICKHOUSE_TMP:-.}" "${CLICKHOUSE_TEST_NAME}_config.XXXXXX.xml")
trap 'rm -f "$local_config"' EXIT
cat > "$local_config" <<'XML'
<clickhouse>
    <max_server_memory_usage>0</max_server_memory_usage>
    <max_server_memory_usage_to_ram_ratio>100</max_server_memory_usage_to_ram_ratio>
    <additional_memory_tracking_per_thread>18446744073709551615</additional_memory_tracking_per_thread>
</clickhouse>
XML

${CLICKHOUSE_LOCAL} --config-file "$local_config" --query "
    SELECT
        value != '18446744073709551615',
        toUInt64(value) > 16777216 AND toUInt64(value) <= ${os_memory_total},
        changeable_without_restart
    FROM system.server_settings
    WHERE name = 'additional_memory_tracking_per_thread'
    SETTINGS max_threads = 1"
