#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug, no-llvm-coverage
# NOTE: jemalloc profiling (needed by the profile tables) is unavailable in these builds

# `SYSTEM JEMALLOC` is required to read the `system.jemalloc_*` tables: explicit `SELECT` on them is not
# enough, and `SYSTEM JEMALLOC` alone is sufficient (`select_from_system_db_requires_grant` is enabled in CI).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_05238_${CLICKHOUSE_DATABASE}"
TABLES="jemalloc_bins jemalloc_arena_bins jemalloc_stats jemalloc_profile_text jemalloc_sampled_allocations"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} IDENTIFIED WITH no_password"

echo "no grants"
for table in $TABLES; do
    $CLICKHOUSE_CLIENT --user="${USER}" --query "SELECT count() FROM system.${table}" 2>&1 | grep -m1 -o 'ACCESS_DENIED'
done

echo "SELECT only"
for table in $TABLES; do
    $CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.${table} TO ${USER}"
    $CLICKHOUSE_CLIENT --user="${USER}" --query "SELECT count() FROM system.${table}" 2>&1 | grep -m1 -o 'ACCESS_DENIED'
done

echo "SYSTEM JEMALLOC only"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON system.* FROM ${USER}"
$CLICKHOUSE_CLIENT --query "GRANT SYSTEM JEMALLOC ON *.* TO ${USER}"
for table in $TABLES; do
    $CLICKHOUSE_CLIENT --user="${USER}" --jemalloc_profile_text_output_format=raw --query "SELECT '${table}', count() >= 0 FROM system.${table}"
done

$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
