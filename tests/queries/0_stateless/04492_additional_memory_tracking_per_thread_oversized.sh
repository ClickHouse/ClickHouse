#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the `additional_memory_tracking_per_thread` overflow guard.
#
# The setting is `UInt64`, but its value is consumed as a signed `int64_t` delta
# that the pipeline executor adds straight into the total `MemoryTracker`
# (`will_be = size + amount.fetch_add(size)`). Clamping a misconfigured huge
# value at `INT64_MAX` is NOT safe: on a running server `amount`/`rss` are
# already positive, so a near-`INT64_MAX` reservation overflows that signed
# addition, wraps negative and corrupts the tracker before the hard-limit check
# can reject it. With a corrupted (negative) total, the limit check passes and
# the query wrongly succeeds while leaving server memory accounting broken.
#
# The fix clamps the value to the physical server memory, so even an absurd
# `UInt64` value produces a finite, overflow-safe reservation. A single such
# reservation (>= total RAM) is necessarily above any sane `max_server_memory_usage`,
# so the very first pipeline worker trips the limit and the query fails cleanly
# with `MEMORY_LIMIT_EXCEEDED` instead of corrupting the tracker.
#
# We run `clickhouse-local` with a private config so the oversized value does
# not affect the shared stateless-test server.

CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04492_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE"' EXIT

# Decouple the total memory tracker from the machine's state, the same way as
# `04825_additional_memory_tracking_per_thread_push_executor`, so that only the
# speculative reservation can push it over the limit:
#   * in CI many tests share one cgroup, so the cgroup-based RSS correction
#     would feed the combined memory usage of every concurrently running test
#     into this process's total memory tracker - pin it to this process's own
#     RSS instead;
#   * the dynamic hard-limit adjustment recomputes the limit from the host's
#     available memory on every tick - keep the limit static;
#   * the speculative RSS reserve extrapolates RSS growth on top of the
#     observed value - disable it so the published RSS is exact.
MEMORY_WORKER_CONFIG="<memory_worker_use_cgroup>false</memory_worker_use_cgroup>
    <memory_worker_dynamic_hard_limit>0</memory_worker_dynamic_hard_limit>
    <memory_worker_rss_speculative_reserve_ratio>0</memory_worker_rss_speculative_reserve_ratio>"

# `clickhouse-local` exposes the effective cgroup-aware default hard limit.
# Derive the thresholds from it so the test is independent of the machine's
# memory size: a hard-coded limit could be clamped below the hard-coded
# reservation on a small cgroup, or exceeded before the first reservation.
DEFAULT_MAX_SERVER_MEMORY_USAGE=$(${CLICKHOUSE_LOCAL} --query "SELECT getServerSetting('max_server_memory_usage')")
FAILING_LIMIT=$((DEFAULT_MAX_SERVER_MEMORY_USAGE / 2))

# `additional_memory_tracking_per_thread` is set to `UInt64` max, far above
# `INT64_MAX`; it must be clamped to the physical server memory rather than
# wrapping negative. The configured hard limit is half of the default one,
# which itself never exceeds the physical memory, so the clamped reservation
# (>= total RAM) exceeds it on the first worker.
cat > "$CONFIG_FILE" <<EOF
<clickhouse>
    ${MEMORY_WORKER_CONFIG}
    <max_server_memory_usage>${FAILING_LIMIT}</max_server_memory_usage>
    <additional_memory_tracking_per_thread>18446744073709551615</additional_memory_tracking_per_thread>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    SELECT count() FROM numbers_mt(1000) SETTINGS max_threads = 8
" 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1
