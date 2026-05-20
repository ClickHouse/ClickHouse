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
#   * `max_server_memory_usage = 1G` -- a hard cap small enough that the
#     speculative reservation alone exceeds it.
#   * `additional_memory_tracking_per_thread = 1G` -- every job queued to a
#     `ThreadPool` worker reserves 1 GiB up front.
#   * `max_threads = 8` -- so the eight pipeline jobs together speculatively
#     reserve 8 GiB, far above the 1 GiB hard limit.
#
# `max_untracked_memory` is left at its default (4 MiB), so the query itself
# does not touch the per-query limit. The only path that can fail is the
# speculative reservation in the pipeline executors -- if it is broken (for
# example throws outside the lambda's `try`/`catch`), the query hangs or
# crashes; if it is correct, it surfaces as a normal `MEMORY_LIMIT_EXCEEDED`.

CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04240_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE"' EXIT

cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>1073741824</max_server_memory_usage>
    <additional_memory_tracking_per_thread>1073741824</additional_memory_tracking_per_thread>
</clickhouse>
EOF

# Eight worker jobs * 1 GiB speculative each = 8 GiB > 1 GiB hard limit; the
# pipeline executor must abort the query with MEMORY_LIMIT_EXCEEDED instead of
# hanging.
${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    SELECT count() FROM numbers_mt(1000) SETTINGS max_threads = 8
" 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1
