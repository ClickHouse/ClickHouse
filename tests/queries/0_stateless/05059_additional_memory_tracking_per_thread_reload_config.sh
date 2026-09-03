#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `SYSTEM RELOAD CONFIG` is global server state, and the test
# temporarily changes the server-wide memory settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `additional_memory_tracking_per_thread` is changeable without a restart. The value
# `system.server_settings` reports must be the live effective one: read back from the
# atomic the pipeline executor consults, not the startup or the raw configured value.
# In particular, an oversized value is clamped to the physical server memory on
# reload, and `system.server_settings` must show the clamped value.

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

function hard_limit()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT toUInt64(value)
        FROM system.server_settings
        WHERE name = 'max_server_memory_usage'
        SETTINGS max_threads = 1"
}

function restore_config()
{
    rm -f "$config_path"
    reload_config
}
trap restore_config EXIT

baseline=$(show_setting)
hard_limit_before=$(hard_limit)
echo "$baseline" | cut -f2

# 1. An explicit value takes effect on reload and is reported as-is.
cat > "$config_path" <<'XML'
<clickhouse>
    <additional_memory_tracking_per_thread>8388608</additional_memory_tracking_per_thread>
</clickhouse>
XML
reload_config
show_setting

# 2. Raise the server-wide memory limit before reserving an oversized amount, so the
#    test's own queries can still run while every pipeline worker reserves the whole
#    physical memory. An explicit `max_server_memory_usage` would cap the ratio, so it
#    is reset to "derive from the ratio". The dynamic limiter only lowers the hard
#    limit on reload and raises it toward the new ceiling on its next tick, so wait
#    for the raise.
cat > "$config_path" <<'XML'
<clickhouse>
    <additional_memory_tracking_per_thread>8388608</additional_memory_tracking_per_thread>
    <max_server_memory_usage>0</max_server_memory_usage>
    <max_server_memory_usage_to_ram_ratio>1000</max_server_memory_usage_to_ram_ratio>
</clickhouse>
XML
reload_config
for _ in $(seq 1 200)
do
    current=$(hard_limit)
    if [ "$hard_limit_before" -eq 0 ] || [ "${current:-0}" -gt $((hard_limit_before * 10)) ]
    then
        break
    fi
    sleep 0.5
done

# 3. An oversized value (`UInt64` max, far above `INT64_MAX`) is clamped to the
#    physical server memory: the reported live value is finite, positive, fits a
#    signed 64-bit delta, and differs from the raw configured value. The exact
#    physical memory is not asserted because it depends on the cgroup of the server.
cat > "$config_path" <<'XML'
<clickhouse>
    <additional_memory_tracking_per_thread>18446744073709551615</additional_memory_tracking_per_thread>
    <max_server_memory_usage>0</max_server_memory_usage>
    <max_server_memory_usage_to_ram_ratio>1000</max_server_memory_usage_to_ram_ratio>
</clickhouse>
XML
reload_config
$CLICKHOUSE_CLIENT --query "
    SELECT
        value != '18446744073709551615',
        toUInt64(value) BETWEEN 1 AND 9223372036854775807,
        changeable_without_restart
    FROM system.server_settings
    WHERE name = 'additional_memory_tracking_per_thread'
    SETTINGS max_threads = 1"

# 4. Removing the override restores the startup value on the next reload.
trap - EXIT
restore_config
if [ "$(show_setting)" == "$baseline" ]
then
    echo "restored"
else
    echo "not restored: baseline '$baseline', now '$(show_setting)'"
fi
