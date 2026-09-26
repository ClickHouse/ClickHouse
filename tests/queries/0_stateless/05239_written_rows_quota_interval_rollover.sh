#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An insert accounts `written_rows` and `written_bytes` of the same chunk together. When the first insert
# of a new quota interval overflows the stale `written_bytes` of the ended interval, the interval is reset.
# That reset must not wipe the `written_rows` of the same chunk, which was accounted just before.
# Quotas, users and roles are server-global, so the names are made unique.

ROLE="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUOTA="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="rollover_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ROW="'a long string that costs many bytes but only one row'"

${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${TABLE} (s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} TO ${USER}"

# Measure how many bytes one inserted block costs.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR TRACKING ONLY TO ${ROLE}"
${CLICKHOUSE_CLIENT} --user ${USER} -q "INSERT INTO ${TABLE} VALUES (${ROW})"
BLOCK_BYTES=$(${CLICKHOUSE_CLIENT} -q "SELECT written_bytes FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${TABLE}"

# One block fits into `written_bytes`, two blocks do not.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 5 SECOND MAX written_rows = 1000, written_bytes = $((BLOCK_BYTES * 3 / 2)) TO ${ROLE}"

# Wait for the interval to end without querying the quota, because a query of `system.quotas_usage`
# after the end would start the new interval by itself.
function wait_until()
{
    while [ "$(date +%s)" -lt "$1" ]; do
        sleep 0.1
    done
}

# Start at the beginning of an interval, so that the first insert surely belongs to it.
wait_until "$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp(end_time) FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")"

# Both inserts run in one session: a successful login starts the new interval by itself, which would hide the problem.
# The sleeps cross the end of the interval, then the second insert is the first accounting of the new interval,
# and 2 * `BLOCK_BYTES` overflows the stale `written_bytes` of the ended interval.
${CLICKHOUSE_CLIENT} --user ${USER} -q "
    INSERT INTO ${TABLE} VALUES (${ROW});
    SELECT sleep(3) FORMAT Null;
    SELECT sleep(2.5) FORMAT Null;
    INSERT INTO ${TABLE} VALUES (${ROW});
"

${CLICKHOUSE_CLIENT} -q "SELECT written_rows, written_bytes = ${BLOCK_BYTES} FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
