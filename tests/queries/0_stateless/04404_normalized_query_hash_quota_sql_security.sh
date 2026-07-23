#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `KEYED BY normalized_query_hash` quota assigned to the definer of a `SQL SECURITY DEFINER` materialized
# view must keep bucketing resources per query pattern even though the dependent insert runs under a fresh
# SQL-security-overridden context. That context is created from the global context, where the normalized
# query hash would otherwise be 0, so without carrying the outer hash all query patterns shared a single
# hash-0 bucket. Here the `written_bytes` of the materialized-view insert are accounted to the definer's
# quota, bucketed by the *outer* insert pattern (the literal in `numbers()` is normalized away, so
# `numbers(7)`/`numbers(13)` share a hash, while `number * 2` is a structurally different pattern with its
# own bucket).
#
# Quotas and users are server-global, so the names are suffixed with the unique database name to keep the
# test isolated when it runs in parallel with itself (e.g. in the flaky check).

definer="d_04404_${CLICKHOUSE_DATABASE}"
caller="c_04404_${CLICKHOUSE_DATABASE}"
quota="q_04404_${CLICKHOUSE_DATABASE}"

# shellcheck disable=SC2086  # CLICKHOUSE_CLIENT must be word-split.
check_exceeded() { ${CLICKHOUSE_CLIENT} --user "${caller}" --send_logs_level=none -q "$1" 2>&1 | grep -m1 -o QUOTA_EXCEEDED || echo allowed; }

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS mv_04404"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_04404"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS tgt_04404"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${caller}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${definer}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${definer}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${caller}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, INSERT ON *.* TO ${definer}"

# The source is `Null` so only the materialized-view write is accounted; the view runs as the definer.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE src_04404 (x UInt64) ENGINE = Null"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE tgt_04404 (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_04404 TO tgt_04404 DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_04404"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON src_04404 TO ${caller}"

echo "=== written_bytes of a SQL SECURITY DEFINER materialized view is bucketed per outer insert hash ==="
# Each materialized row is a UInt64 = 8 bytes (the counting transform counts the in-memory block size).
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX written_bytes = 80 TO ${definer}"

echo "--- pattern A, first insert (7 rows = 56 bytes) is within the per-pattern limit ---"
check_exceeded "INSERT INTO src_04404 SELECT number FROM numbers(7)"
echo "--- pattern A, second insert (cumulative 160 > 80) exceeds the limit of its own bucket ---"
check_exceeded "INSERT INTO src_04404 SELECT number FROM numbers(13)"
echo "--- pattern B (a structurally different insert) has its own bucket and is still allowed ---"
check_exceeded "INSERT INTO src_04404 SELECT number * 2 FROM numbers(7)"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS mv_04404"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src_04404"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS tgt_04404"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${caller}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${definer}"
