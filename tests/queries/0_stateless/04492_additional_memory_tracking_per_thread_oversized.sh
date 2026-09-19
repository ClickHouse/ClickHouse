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

# `additional_memory_tracking_per_thread` is set to `UInt64` max, far above
# `INT64_MAX`; it must be clamped to the physical server memory rather than
# wrapping negative. `max_server_memory_usage = 1G` is small enough that the
# clamped reservation (>= total RAM) exceeds it on the first worker.
cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>1073741824</max_server_memory_usage>
    <additional_memory_tracking_per_thread>18446744073709551615</additional_memory_tracking_per_thread>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --query "
    SELECT count() FROM numbers_mt(1000) SETTINGS max_threads = 8
" 2>&1 | grep -oE 'MEMORY_LIMIT_EXCEEDED' | head -n1
