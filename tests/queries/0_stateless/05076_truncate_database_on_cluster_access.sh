#!/usr/bin/env bash
# TRUNCATE of a database is authorized by `DROP DATABASE`, the same privilege the local spelling
# requires. The `ON CLUSTER` form is checked on the initiator before the task is queued, so a
# refused statement leaves the database untouched.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLUSTER="test_shard_localhost"
# Users are server-global, so their names carry the test database to keep this test safe against a
# concurrent copy of itself.
DB="db_05076_${CLICKHOUSE_TEST_UNIQUE_NAME}"
READER="reader_05076_${CLICKHOUSE_TEST_UNIQUE_NAME}"
OWNER="owner_05076_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -mq "
        DROP DATABASE IF EXISTS ${DB} SYNC;
        DROP USER IF EXISTS ${READER}, ${OWNER};
    "
}
cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -mq "
    CREATE USER ${READER} IDENTIFIED WITH no_password;
    CREATE USER ${OWNER} IDENTIFIED WITH no_password;
    GRANT CLUSTER ON *.* TO ${READER}, ${OWNER};
"

function fixture()
{
    ${CLICKHOUSE_CLIENT} -mq "
        DROP DATABASE IF EXISTS ${DB} SYNC;
        CREATE DATABASE ${DB};
        CREATE TABLE ${DB}.t (x UInt32) ENGINE = MergeTree ORDER BY x;
        INSERT INTO ${DB}.t VALUES (1), (2), (3);
        GRANT SELECT ON ${DB}.* TO ${READER};
        GRANT DROP DATABASE ON ${DB}.* TO ${OWNER};
    "
}

# The message names the privilege that was required, so a check that asks for a different one is
# caught here and not only by the outcome.
echo "-- TRUNCATE DATABASE ON CLUSTER, without DROP DATABASE"
fixture
${CLICKHOUSE_CLIENT} --user "${READER}" --query "TRUNCATE DATABASE ${DB} ON CLUSTER ${CLUSTER}" 2>&1 |
    grep -m1 -o -F "necessary to have the grant DROP DATABASE"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t"

# The second parse shape of the same statement: it names tables, so it empties them instead of
# dropping them. It reaches the same authorization branch.
echo "-- TRUNCATE ALL TABLES FROM ON CLUSTER, without DROP DATABASE"
fixture
${CLICKHOUSE_CLIENT} --user "${READER}" --query "TRUNCATE ALL TABLES FROM ${DB} ON CLUSTER ${CLUSTER}" 2>&1 |
    grep -m1 -o -F "necessary to have the grant DROP DATABASE"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB}.t"

# `readonly` is passed on the command line so that it is applied together with the settings the test
# harness already sends: constraints are checked against the value in force before the query, so
# both are accepted and the statement itself runs readonly. A user-level `readonly = 1` would reject
# those settings first, and this assertion would then hold without the statement ever being
# authorized.
echo "-- TRUNCATE DATABASE ON CLUSTER, with DROP DATABASE but readonly"
fixture
${CLICKHOUSE_CLIENT} --user "${OWNER}" --readonly 1 --query "TRUNCATE DATABASE ${DB} ON CLUSTER ${CLUSTER}" 2>&1 |
    grep -m1 -o -F "Cannot execute query in readonly mode"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t"

# `DROP DATABASE` is enough on its own: a check that required anything besides it would refuse here.
echo "-- TRUNCATE DATABASE ON CLUSTER, with DROP DATABASE"
fixture
${CLICKHOUSE_CLIENT} --user "${OWNER}" --distributed_ddl_output_mode none --query "TRUNCATE DATABASE ${DB} ON CLUSTER ${CLUSTER}"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t"
