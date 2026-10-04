#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Validate the `additional_memory_tracking_per_thread` speculative reservation
# directly, i.e. without relying on `max_untracked_memory = 0` to force the
# same exception path. We run `clickhouse-local` with a private config so we
# can dial `max_server_memory_usage` and `additional_memory_tracking_per_thread`
# to values that make the speculative reservation alone exceed the limit,
# without affecting the shared stateless-test server.
#
# Setup:
#   * `max_server_memory_usage` is half of the default hard limit -- a cap small
#     enough that a single speculative reservation alone exceeds it.
#   * `additional_memory_tracking_per_thread` is the full default hard limit --
#     every pipeline worker reserves it up front, so even one reservation is
#     above the configured hard limit. We deliberately make a single reservation exceed the limit instead
#     of relying on several reservations overlapping: the failure is then
#     independent of how the workers are scheduled (they could otherwise run
#     sequentially enough that each reservation is freed before the next one
#     pushes total memory over the limit, making the test flaky).
#   * `max_threads = 8` -- there is plenty of headroom; the very first pipeline
#     worker that runs already trips the limit.
#
# `max_untracked_memory` is left at its default (4 MiB), so the query itself
# does not touch the per-query limit. The only path that can fail is the
# speculative reservation in the pipeline executors -- if it is broken (for
# example throws outside the lambda's `try`/`catch`), the query hangs or
# crashes; if it is correct, it surfaces as a normal `MEMORY_LIMIT_EXCEEDED`.

CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04491_config.XXXXXX.xml)
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
FAILING_RESERVATION=$DEFAULT_MAX_SERVER_MEMORY_USAGE

cat > "$CONFIG_FILE" <<EOF
<clickhouse>
    ${MEMORY_WORKER_CONFIG}
    <max_server_memory_usage>${FAILING_LIMIT}</max_server_memory_usage>
    <additional_memory_tracking_per_thread>${FAILING_RESERVATION}</additional_memory_tracking_per_thread>
</clickhouse>
EOF

# A single speculative reservation already exceeds the hard limit;
# the pipeline executor must abort the query with MEMORY_LIMIT_EXCEEDED instead
# of hanging, regardless of how many workers run concurrently.
${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    SELECT count() FROM numbers_mt(1000) SETTINGS max_threads = 8
" 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1
