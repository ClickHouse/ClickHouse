#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user can be assigned several quotas at once (here, of different key types). All of them must
# be enforced together: a query is rejected if ANY assigned quota is exceeded. Previously only one
# quota per user was enforced, chosen non-deterministically, so the others were silently ignored.
#
# Note: queries like `SELECT 1` use `system.one`, which is exempt from quotas, so the test uses
# `numbers()` and a real table.

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_04321"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q_04321_hash"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q_04321_user"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04321"

${CLICKHOUSE_CLIENT} -q "CREATE USER u_04321"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04321 (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO u_04321"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON t_04321 TO u_04321"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW QUOTAS ON *.* TO u_04321"

# Two quotas with different key types, both assigned to the same user. Each limits a different
# resource, so producing the expected output requires BOTH to be enforced at the same time.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q_04321_hash KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX query_selects = 2 TO u_04321"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q_04321_user KEYED BY user_name FOR INTERVAL 100 YEAR MAX query_inserts = 2 TO u_04321"

echo "--- both quotas are enforced for the user (system.quota_usage shows both) ---"
${CLICKHOUSE_CLIENT} --user u_04321 -q "SELECT quota_name FROM system.quota_usage WHERE quota_name IN ('q_04321_hash', 'q_04321_user') ORDER BY quota_name"

echo "--- normalized_query_hash quota: query_selects = 2 per query pattern is enforced ---"
${CLICKHOUSE_CLIENT} --user u_04321 -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u_04321 -q "SELECT number FROM numbers(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --user u_04321 --send_logs_level=none -q "SELECT number FROM numbers(1) FORMAT Null" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

echo "--- user_name quota: query_inserts = 2 is enforced (the second quota is not dead weight) ---"
${CLICKHOUSE_CLIENT} --user u_04321 -q "INSERT INTO t_04321 VALUES (1)"
${CLICKHOUSE_CLIENT} --user u_04321 -q "INSERT INTO t_04321 VALUES (2)"
${CLICKHOUSE_CLIENT} --user u_04321 --send_logs_level=none -q "INSERT INTO t_04321 VALUES (3)" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_04321"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q_04321_hash"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q_04321_user"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04321"
