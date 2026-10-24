#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: the hook skips tables of a `Replicated` database
# no-fasttest: the fast test runs with `--no-long` and never runs the reattach suite (see `02461_reattach_tables.lib`)

# The internal `DETACH`/`ATTACH` pair of `reattach_tables_before_query_execution` must not consume the
# user's quota, and a `SELECT` that may be answered from the query result cache must not reattach its tables.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_quota"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_quota (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_quota VALUES (1)"

# Control: the table is reattached without the query result cache.
# The hook skips a table whose exclusive lock it cannot take within a short probe, and on a loaded runner
# a background task of the fresh table can hold its lock for that long. The
# query is read-only, so retry it until the hook gets the table idle.
for _ in {1..10}
do
    result=$(check_if_detached "SELECT * FROM t_reattach_quota FORMAT Null" t_reattach_quota)
    [ "$result" = "OK" ] && break
done
echo "$result"

# A `SELECT` with `use_query_cache` may be served from the cache without touching the table.
check_if_not_detached "SELECT * FROM t_reattach_quota SETTINGS use_query_cache = 1 FORMAT Null" t_reattach_quota

# A user with exactly one query left in the quota: the internal `DETACH`/`ATTACH` must not spend it, otherwise
# the `ATTACH` fails with `QUOTA_EXCEEDED` and leaves the table detached.
REATTACH_USER="user_reattach_quota_${CLICKHOUSE_DATABASE}"
REATTACH_QUOTA="quota_reattach_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${REATTACH_QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${REATTACH_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${REATTACH_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${REATTACH_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${REATTACH_USER}"

# The table can be skipped by the busy probe as above, so retry; the quota is re-created for every attempt,
# so that each one starts with exactly one query left.
for _ in {1..10}
do
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${REATTACH_QUOTA}"
    ${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${REATTACH_QUOTA} FOR INTERVAL 1 hour MAX queries = 1 TO ${REATTACH_USER}"

    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${REATTACH_USER}" \
        --reattach_tables_before_query_execution=1 \
        --query "SELECT count() FROM t_reattach_quota" 2>&1)
    REATTACH_STATUS=$?
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        result="FAIL (client error: $REATTACH_OUTPUT)"
        break
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_quota"; then
        result="OK"
        break
    else
        result="FAIL (not detached: $REATTACH_OUTPUT)"
    fi
done
echo "$result"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_quota"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${REATTACH_QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${REATTACH_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_quota"
