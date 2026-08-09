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

cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>1073741824</max_server_memory_usage>
    <additional_memory_tracking_per_thread>2147483648</additional_memory_tracking_per_thread>
</clickhouse>
EOF

# The caller thread of the step-driven INSERT pipeline must acquire the 2 GiB
# reservation on the first `executeStep` and fail with MEMORY_LIMIT_EXCEEDED;
# if `executeStep` carries no reservation, the INSERT succeeds and nothing is
# printed.
${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t SETTINGS max_threads = 1, max_insert_threads = 1 VALUES (1);
" < /dev/null 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1

# Control: with a hard limit far above the reservation (and above any realistic
# baseline memory usage of `clickhouse-local`), the same step-driven INSERT must
# succeed — the reservation is acquired on the first `executeStep`, held for the
# pipeline lifetime, and released at finalization without hanging or leaking.
cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>107374182400</max_server_memory_usage>
    <additional_memory_tracking_per_thread>2147483648</additional_memory_tracking_per_thread>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t SETTINGS max_threads = 1, max_insert_threads = 1 VALUES (1);
    SELECT count() FROM t;
" < /dev/null
