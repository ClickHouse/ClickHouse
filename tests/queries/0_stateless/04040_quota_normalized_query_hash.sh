#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test Feature 1: NORMALIZED_QUERY_HASH as a quota key type
# Test Feature 2: QUERIES_PER_NORMALIZED_HASH as a quota resource type

# Note: queries like SELECT 1 use system.one, which bypasses quota checks.
# We must use queries with explicit FROM clauses to non-exempt tables.

# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04040_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04040_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04040_norm_key_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04040_per_hash_${CLICKHOUSE_DATABASE}"

# Setup: create a dedicated user/role so the default XML quota doesn't interfere.
${CLICKHOUSE_CLIENT} -q "CREATE ROLE r04040_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER u04040_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO r04040_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT r04040_${CLICKHOUSE_DATABASE} TO u04040_${CLICKHOUSE_DATABASE}"

# ============================================================
# Feature 1: KEYED BY normalized_query_hash
# Each distinct normalized query gets its own quota bucket.
# ============================================================

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q04040_norm_key_${CLICKHOUSE_DATABASE} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX queries = 3 TO r04040_${CLICKHOUSE_DATABASE}"

# Verify SHOW CREATE QUOTA roundtrips correctly.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE QUOTA q04040_norm_key_${CLICKHOUSE_DATABASE}" | sed "s/_${CLICKHOUSE_DATABASE}//g"

# Pattern A: SELECT number FROM numbers(1) — 3 times should succeed, 4th should fail.
# normalizedQueryHash replaces literals, so numbers(1) and numbers(2) have the same hash.
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} --send_logs_level=none -q "SELECT number FROM numbers(1) FORMAT Null" 2>&1 | grep -o 'QUOTA_EXCEEDED'

# Pattern B: structurally different query (two columns instead of one).
# Has a different normalized hash, so gets its own bucket.
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number, number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number, number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number, number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} --send_logs_level=none -q "SELECT number, number FROM numbers(1) FORMAT Null" 2>&1 | grep -o 'QUOTA_EXCEEDED'

# Cleanup Feature 1.
${CLICKHOUSE_CLIENT} -q "DROP QUOTA q04040_norm_key_${CLICKHOUSE_DATABASE}"

# ============================================================
# Feature 2: MAX queries_per_normalized_hash
# Limits the maximum number of executions of any single
# normalized query, regardless of the quota key type.
# ============================================================

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q04040_per_hash_${CLICKHOUSE_DATABASE} FOR INTERVAL 100 YEAR MAX queries_per_normalized_hash = 2 TO r04040_${CLICKHOUSE_DATABASE}"

# Verify SHOW CREATE QUOTA roundtrips correctly.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE QUOTA q04040_per_hash_${CLICKHOUSE_DATABASE}" | sed "s/_${CLICKHOUSE_DATABASE}//g"

# Pattern C: 2 times should succeed, 3rd should fail.
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} --send_logs_level=none -q "SELECT number FROM numbers(1) FORMAT Null" 2>&1 | grep -o 'QUOTA_EXCEEDED'

# Pattern D: structurally different query, separate per-hash counter.
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number, number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} -q "SELECT number, number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u04040_${CLICKHOUSE_DATABASE} --send_logs_level=none -q "SELECT number, number FROM numbers(1) FORMAT Null" 2>&1 | grep -o 'QUOTA_EXCEEDED'

# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04040_per_hash_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04040_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04040_${CLICKHOUSE_DATABASE}"
