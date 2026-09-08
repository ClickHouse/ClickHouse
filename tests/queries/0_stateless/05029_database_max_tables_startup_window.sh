#!/usr/bin/env bash
# Tests that the `max_tables` preflight checks in the cross-database `RENAME` and `UNDROP` paths
# do not destroy anything when the destination database is attached concurrently. `ATTACH DATABASE`
# publishes the database in the catalog before its tables are loaded, so a preflight that does not
# account for that can see a partial table list, let the operation past the point of no return, and
# only then fail with `TOO_MANY_TABLES` -- losing the source table or the dropped-table record.
#
# NOTE: the test config sets `database_atomic_delay_before_drop_table_sec` to 60, after which the
# dropped table is cleaned up for real and disappears from `system.dropped_tables`. Everything
# between the `DROP TABLE` and the checks below is batched into a few client invocations to stay
# well below that limit even with sanitizers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The test creates an `Ordinary` database, and the server keeps warning about it.
CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level none"

FULL_DB="${CLICKHOUSE_DATABASE}_full"
ORDINARY_DB="${CLICKHOUSE_DATABASE}_ordinary"

${CLIENT} -q "DROP DATABASE IF EXISTS ${FULL_DB}"
${CLIENT} -q "DROP DATABASE IF EXISTS ${ORDINARY_DB}"

${CLIENT} -q "CREATE DATABASE ${FULL_DB} ENGINE = Atomic"
${CLIENT} --allow_deprecated_database_ordinary 1 -q "CREATE DATABASE ${ORDINARY_DB} ENGINE = Ordinary"

# Enough tables that loading the database takes a while, and more than the limit set below.
{
    for i in $(seq 1 30); do
        echo "CREATE TABLE ${FULL_DB}.t${i} (x UInt32) ENGINE = Null;"
    done
    # This table is dropped asynchronously below, so it stays in the dropped-table queue for `UNDROP`.
    echo "CREATE TABLE ${FULL_DB}.undrop_me (x UInt32) ENGINE = MergeTree ORDER BY x;"
    # The database is now over its limit: neither a rename into it nor an `UNDROP` may succeed.
    echo "ALTER DATABASE ${FULL_DB} MODIFY SETTING max_tables = 20;"
    echo "CREATE TABLE ${ORDINARY_DB}.src (x UInt32) ENGINE = Null;"
} | ${CLIENT}

${CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 0 -q "DROP TABLE ${FULL_DB}.undrop_me"

${CLIENT} -q "DETACH DATABASE ${FULL_DB}"
${CLIENT} -q "ATTACH DATABASE ${FULL_DB}" &
attach_pid=$!

# Race both operations against the attach. They must always fail, and must never destroy anything.
{
    for _ in $(seq 1 10); do
        echo "RENAME TABLE ${ORDINARY_DB}.src TO ${FULL_DB}.src;"
        echo "UNDROP TABLE ${FULL_DB}.undrop_me;"
    done
} | ${CLIENT} --ignore-error 2>/dev/null

wait $attach_pid

# The source table of the failed rename is still where it was.
${CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${ORDINARY_DB}' AND name = 'src'"
# The failed `UNDROP` did not remove the table from the dropped-table queue.
${CLIENT} -q "SELECT count() FROM system.dropped_tables WHERE database = '${FULL_DB}' AND table = 'undrop_me'"
# The limit is still enforced now that the database has finished loading.
${CLIENT} -q "RENAME TABLE ${ORDINARY_DB}.src TO ${FULL_DB}.src" 2>&1 | grep -q -F 'TOO_MANY_TABLES' && echo 1
${CLIENT} -q "UNDROP TABLE ${FULL_DB}.undrop_me" 2>&1 | grep -q -F 'TOO_MANY_TABLES' && echo 1

${CLIENT} -q "DROP DATABASE ${FULL_DB}"
${CLIENT} --force_remove_data_recursively_on_drop 1 -q "DROP DATABASE ${ORDINARY_DB}"
