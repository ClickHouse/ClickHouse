#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lower the client log level so transient server warnings (e.g. QuotaCache's "Re-chose quotas
# ... in N ms", emitted when a recompute is slow under load) cannot leak onto stderr and fail
# the stdout-only assertions below.
CLICKHOUSE_CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=error/")

# A user can be assigned several quotas at once (here, of different key types). All of them must
# be enforced together: a query is rejected if ANY assigned quota is exceeded. Previously only one
# quota per user was enforced, chosen non-deterministically, so the others were silently ignored.
#
# Note: queries like `SELECT 1` use `system.one`, which is exempt from quotas, so the test uses
# `numbers()` and a real table.
#
# Quotas and users are server-global, so the names are suffixed with the (unique) database name to
# keep the test isolated when it runs in parallel with itself (e.g. in the flaky check).

user="u_04321_${CLICKHOUSE_DATABASE}"
quota_hash="q_04321_hash_${CLICKHOUSE_DATABASE}"
quota_user="q_04321_user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_hash}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_user}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04321"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04321 (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON t_04321 TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW QUOTAS ON *.* TO ${user}"

# Two quotas with different key types, both assigned to the same user. Each limits a different
# resource, so producing the expected output requires BOTH to be enforced at the same time.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota_hash} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX query_selects = 2 TO ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota_user} KEYED BY user_name FOR INTERVAL 100 YEAR MAX query_inserts = 2 TO ${user}"

echo "--- both quotas are enforced for the user (system.quota_usage shows both) ---"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT replaceOne(quota_name, '_${CLICKHOUSE_DATABASE}', '') FROM system.quota_usage WHERE quota_name IN ('${quota_hash}', '${quota_user}') ORDER BY quota_name"

# `SHOW CREATE QUOTA CURRENT` must also report every quota governing the session, not a single one
# chosen non-deterministically. The order of the quotas is not guaranteed, so the names are sorted.
echo "--- SHOW CREATE QUOTA CURRENT lists every enforced quota (previously only one) ---"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SHOW CREATE QUOTA CURRENT" | grep -oP '(?<=CREATE QUOTA )\S+' | sed "s/_${CLICKHOUSE_DATABASE}//" | sort

echo "--- normalized_query_hash quota: query_selects = 2 per query pattern is enforced ---"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -q "SELECT number FROM numbers(1) FORMAT Null" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

echo "--- user_name quota: query_inserts = 2 is enforced (the second quota is not dead weight) ---"
${CLICKHOUSE_CLIENT} --user "${user}" -q "INSERT INTO t_04321 VALUES (1)"
${CLICKHOUSE_CLIENT} --user "${user}" -q "INSERT INTO t_04321 VALUES (2)"
${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -q "INSERT INTO t_04321 VALUES (3)" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_hash}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_user}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04321"
