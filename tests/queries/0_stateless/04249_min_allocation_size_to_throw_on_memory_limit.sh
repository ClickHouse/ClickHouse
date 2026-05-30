#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Default: setting is disabled (0).
${CLICKHOUSE_LOCAL} --query "SELECT value FROM system.server_settings WHERE name = 'min_allocation_size_to_throw_on_memory_limit'"

# Setting from server config: clickhouse-local applies it via the same code path as the server.
${CLICKHOUSE_LOCAL} -q "SELECT value FROM system.server_settings WHERE name = 'min_allocation_size_to_throw_on_memory_limit'" -- --min_allocation_size_to_throw_on_memory_limit=1Mi

# Tight memory limit + size-gated throw policy still surfaces MEMORY_LIMIT_EXCEEDED for an oversize aggregation.
# We do not assert which path the throw came from (the query also hits the explicit-alloc path); the point is
# that wiring the new setting alongside `max_server_memory_usage` does not break normal memory-limit reporting.
${CLICKHOUSE_LOCAL} -q "SELECT number FROM system.numbers GROUP BY number HAVING count() > 1 SETTINGS max_bytes_ratio_before_external_group_by = 0" -- --max_server_memory_usage=100Mi --min_allocation_size_to_throw_on_memory_limit=1Mi 2>&1 | grep -o -m1 -E "MEMORY_LIMIT_EXCEEDED"
