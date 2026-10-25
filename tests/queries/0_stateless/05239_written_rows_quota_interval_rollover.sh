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

# The inserts are synchronous: an asynchronous insert is accounted when its batch is flushed, which can be seconds
# later and in another quota interval.
# Measure how many bytes one inserted block costs.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR TRACKING ONLY TO ${ROLE}"
${CLICKHOUSE_CLIENT} --user ${USER} -q "INSERT INTO ${TABLE} SETTINGS async_insert = 0 VALUES (${ROW})"
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

# The scenario depends on the wall clock, and a slow run (e.g. under a sanitizer on a loaded machine) can miss
# the intervals it targets. Each attempt reports the end of the interval for its checkpoints, and an attempt
# whose checkpoints are not in the intended intervals is repeated.
for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${TABLE}"

    # Start at the beginning of an interval, so that the first insert surely belongs to it.
    END=$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp(end_time) FROM system.quotas_usage WHERE quota_name = '${QUOTA}'")
    wait_until "${END}"

    # Both inserts run in one session: a successful login starts the new interval by itself, which would hide the problem.
    # The sleeps cross the end of the interval, then the second insert is the first accounting of the new interval,
    # and 2 * `BLOCK_BYTES` overflows the stale `written_bytes` of the ended interval. Queries reading only tables of the `system` database
    # (including `system.quota_usage`) are not accounted by quotas and do not start the new interval.
    # The usage is read in the same session, because a new client may start too late on a slow machine.
    # A slow attempt may also exceed the quota by accounting both inserts in one interval, so the errors are
    # ignored, and such an attempt is repeated as it does not report both checkpoints.
    RESULT=$(${CLICKHOUSE_CLIENT} --user ${USER} 2>/dev/null -q "
        INSERT INTO ${TABLE} SETTINGS async_insert = 0 VALUES (${ROW});
        SELECT toUnixTimestamp(end_time) FROM system.quota_usage WHERE quota_name = '${QUOTA}';
        SELECT sleep(3) FORMAT Null;
        SELECT sleep(2.5) FORMAT Null;
        INSERT INTO ${TABLE} SETTINGS async_insert = 0 VALUES (${ROW});
        SELECT toUnixTimestamp(end_time), written_rows, written_bytes = ${BLOCK_BYTES} FROM system.quota_usage WHERE quota_name = '${QUOTA}' FORMAT TSV;
    ")

    read -r -d '' FIRST_END SECOND_END WRITTEN_ROWS WRITTEN_BYTES_OK <<< "${RESULT}"
    if [ "${FIRST_END}" = "$((END + 5))" ] && [ "${SECOND_END}" = "$((END + 10))" ]; then
        echo -e "${WRITTEN_ROWS}\t${WRITTEN_BYTES_OK}"
        break
    fi
done

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
