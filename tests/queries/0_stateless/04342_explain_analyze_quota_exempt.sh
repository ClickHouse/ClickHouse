#!/usr/bin/env bash
# Tags: no-parallel
# Reason: modifies quotas and creates users

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# EXPLAIN ANALYZE executes the inner SELECT, so it must charge resource quotas exactly as
# running that SELECT directly would. Special system tables (e.g. system.one) set
# ignore_quota/ignore_limits during planning and are exempt from quotas, so EXPLAIN ANALYZE
# over them must NOT charge the quota either, while a regular source must still be charged.

${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04342"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04342"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04342"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE r04342"
${CLICKHOUSE_CLIENT} -q "CREATE USER u04342"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO r04342"
${CLICKHOUSE_CLIENT} -q "GRANT r04342 TO u04342"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q04342 KEYED BY user_name FOR INTERVAL 100 YEAR MAX read_rows = 1000000 TO r04342"

# Exempt system table: EXPLAIN ANALYZE must not charge the read_rows quota.
${CLICKHOUSE_CLIENT} --user u04342 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT dummy FROM system.one" > /dev/null 2>&1
echo "read_rows after exempt source:"
${CLICKHOUSE_CLIENT} -q "SELECT sum(read_rows) FROM system.quotas_usage WHERE quota_name = 'q04342'"
echo "exempt query exit code: $?"

# Regular source: EXPLAIN ANALYZE must charge the read_rows quota (reads 5 rows).
${CLICKHOUSE_CLIENT} --user u04342 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(5)" > /dev/null 2>&1
echo "read_rows after regular source:"
${CLICKHOUSE_CLIENT} -q "SELECT sum(read_rows) FROM system.quotas_usage WHERE quota_name = 'q04342'"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04342"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04342"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04342"
