#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Validate that the `additional_memory_tracking_per_thread` speculative
# reservation also covers step-driven pipeline execution, where the calling
# thread is the pipeline's only worker (`PushingPipelineExecutor`, used by
# single-threaded INSERT pipelines; the same `PipelineExecutor::executeStep`
# path serves `PullingPipelineExecutor`).
#
# Like 04491, we run `clickhouse-local` with a private config so we can dial
# `max_server_memory_usage` and `additional_memory_tracking_per_thread` to
# values where a single speculative reservation alone exceeds the hard limit:
#   * `max_server_memory_usage = 1G`
#   * `additional_memory_tracking_per_thread = 2G`
# `max_threads = 1` and `max_insert_threads = 1` pin the INSERT pipeline to
# one thread, so `LocalConnection` drives it through the synchronous
# `PushingPipelineExecutor` (`executeStep` on the caller thread) rather than
# spawning pipeline workers.

CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04825_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE"' EXIT

# Decouple the total memory tracker from the machine's state, so the
# "successful" cases below cannot hit the hard limit spuriously:
#   * in CI many tests share one cgroup, so the cgroup-based RSS correction
#     would feed the combined memory usage of every concurrently running test
#     into this process's total memory tracker — pin it to this process's own
#     RSS instead;
#   * the dynamic hard-limit adjustment recomputes the limit from the host's
#     available memory on every tick, silently shrinking the configured
#     `max_server_memory_usage` on a busy machine — keep the limit static;
#   * the speculative RSS reserve extrapolates RSS growth on top of the
#     observed value — disable it so the published RSS is exact.
MEMORY_WORKER_CONFIG="<memory_worker_use_cgroup>false</memory_worker_use_cgroup>
    <memory_worker_dynamic_hard_limit>0</memory_worker_dynamic_hard_limit>
    <memory_worker_rss_speculative_reserve_ratio>0</memory_worker_rss_speculative_reserve_ratio>"

# `clickhouse-local` exposes the effective cgroup-aware default hard limit.
# Derive every threshold below from it so the test is independent of the
# machine's memory size.
DEFAULT_MAX_SERVER_MEMORY_USAGE=$(${CLICKHOUSE_LOCAL} --query "SELECT getServerSetting('max_server_memory_usage')")
FAILING_LIMIT=$((DEFAULT_MAX_SERVER_MEMORY_USAGE / 2))
FAILING_RESERVATION=$DEFAULT_MAX_SERVER_MEMORY_USAGE
SUCCESSFUL_RESERVATION=$((DEFAULT_MAX_SERVER_MEMORY_USAGE / 4))
NESTED_RESERVATION=$((DEFAULT_MAX_SERVER_MEMORY_USAGE / 2))

cat > "$CONFIG_FILE" <<EOF
<clickhouse>
    ${MEMORY_WORKER_CONFIG}
    <max_server_memory_usage>${FAILING_LIMIT}</max_server_memory_usage>
    <additional_memory_tracking_per_thread>${FAILING_RESERVATION}</additional_memory_tracking_per_thread>
</clickhouse>
EOF

# The caller thread of the step-driven INSERT pipeline must acquire a
# reservation larger than the hard limit while it executes `executeStep` and
# fail with MEMORY_LIMIT_EXCEEDED; if `executeStep` carries no reservation, the
# INSERT succeeds and nothing is printed.
${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t SETTINGS max_threads = 1, max_insert_threads = 1 VALUES (1);
" < /dev/null 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1

# Control: with the default hard limit and a one-quarter reservation, the same
# step-driven INSERT must succeed — each `executeStep` job holds and then
# releases its reservation without hanging or leaking.
cat > "$CONFIG_FILE" <<EOF
<clickhouse>
    ${MEMORY_WORKER_CONFIG}
    <max_server_memory_usage>${DEFAULT_MAX_SERVER_MEMORY_USAGE}</max_server_memory_usage>
    <additional_memory_tracking_per_thread>${SUCCESSFUL_RESERVATION}</additional_memory_tracking_per_thread>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t SETTINGS max_threads = 1, max_insert_threads = 1 VALUES (1);
    SELECT count() FROM t SETTINGS max_threads = 1;
" < /dev/null

# A materialized CTE runs its inner `PushingPipelineExecutor` from an outer
# pipeline worker. Derive the two thresholds from `clickhouse-local`'s effective
# hard limit: one half fits under the full hard limit,
# whereas two reservations on that physical worker do not. The nested executor
# must therefore reuse the outer reservation. This remains valid when a CI
# runner has a small cgroup memory limit.
cat > "$CONFIG_FILE" <<EOF
<clickhouse>
    ${MEMORY_WORKER_CONFIG}
    <max_server_memory_usage>${DEFAULT_MAX_SERVER_MEMORY_USAGE}</max_server_memory_usage>
    <additional_memory_tracking_per_thread>${NESTED_RESERVATION}</additional_memory_tracking_per_thread>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    WITH cte AS MATERIALIZED (SELECT number FROM numbers(100))
    SELECT count() FROM cte
    UNION ALL
    SELECT count() FROM cte
    SETTINGS max_threads = 1;
" < /dev/null
