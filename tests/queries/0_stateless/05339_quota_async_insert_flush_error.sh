#!/usr/bin/env bash
# A failure of the whole deferred flush of asynchronous inserts counts `FailedAsyncInsertQuery`
# against the quota of the submitting user, together with the rest of the flush accounting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05339_${CLICKHOUSE_DATABASE}"
Q="quota_05339_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_05339"
}
cleanup

# The constraint is checked by the pipeline of the flush, so the whole flush fails.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_05339 (a UInt32, CONSTRAINT c CHECK a < 10) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_async_05339 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX FailedAsyncInsertQuery = 1 TO ${U}"

# The quota is exceeded when the consumption is above the limit, so the second failure is still allowed.
for _ in 1 2
do
    ${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t_async_05339 VALUES (100)" 2>&1 | grep -o -m1 "VIOLATED_CONSTRAINT"
    ${CLICKHOUSE_CLIENT} -q "SELECT profile_events['FailedAsyncInsertQuery'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
done
${CLICKHOUSE_CLIENT} --user "${U}" --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t_async_05339 VALUES (1)" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_async_05339"

cleanup
