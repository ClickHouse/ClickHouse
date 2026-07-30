#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `PipelineExecutor` has two drivers, and `additional_memory_tracking_per_thread`
# must reserve on both:
#   * `execute` -- spawns worker threads (or runs the pipeline on the calling
#     thread when `max_threads = 1`); covered by `04491`.
#   * `executeStep` -- runs pipeline work on the calling thread across many
#     calls. `PullingPipelineExecutor` and `PushingPipelineExecutor` drive it,
#     so it carries plain `INSERT ... VALUES`, dictionary loads, mutations and
#     merges. A reservation scoped to a single call would be useless here, so it
#     is held for the lifetime of the executor.
#
# As in `04491`, we use `clickhouse-local` with a private config so the
# reservation alone (2 GiB) exceeds the server memory limit (1 GiB), making the
# outcome independent of thread scheduling, and without touching the shared
# stateless-test server.

CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04656_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE"' EXIT

cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>1073741824</max_server_memory_usage>
    <additional_memory_tracking_per_thread>2147483648</additional_memory_tracking_per_thread>
</clickhouse>
EOF

# `CREATE TABLE` runs no pipeline, so it must succeed: this pins the failure
# below to the pipeline driver rather than to the small server memory limit.
${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory
" < /dev/null 2>&1 | grep -cF 'MEMORY_LIMIT_EXCEEDED'

# `INSERT ... VALUES` is driven by `PushingPipelineExecutor`, i.e. by
# `executeStep`. It must abort with `MEMORY_LIMIT_EXCEEDED` instead of running
# unreserved (or hanging on the pipeline's queue).
${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (1);
" < /dev/null 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1

# With the setting disabled the very same insert must succeed, so the failure
# above is the reservation and not the 1 GiB limit itself.
CONTROL_CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04656_control_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE" "$CONTROL_CONFIG_FILE"' EXIT

cat > "$CONTROL_CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>1073741824</max_server_memory_usage>
    <additional_memory_tracking_per_thread>0</additional_memory_tracking_per_thread>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONTROL_CONFIG_FILE" --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    SELECT count() FROM t;
" < /dev/null 2>&1 | tail -n1
