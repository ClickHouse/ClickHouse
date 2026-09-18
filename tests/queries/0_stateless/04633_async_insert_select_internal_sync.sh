#!/usr/bin/env bash
# Tags: long, no-ordinary-database, no-replicated-database
#   long: a refresh plus several SYSTEM FLUSH LOGS add up under sanitizers.
#   no-ordinary-database: refreshable MV swaps its inner table with EXCHANGE, and CTAS publishes via
#     a temporary table; both need an Atomic database.
#   no-replicated-database: refresh coordination and the user DDL below behave differently there.

# Internal INSERT ... SELECT (refreshable MV, POPULATE, CREATE TABLE ... AS SELECT) must never take the
# async insert queue route. It publishes or swaps its destination as soon as the query returns, so with
# async_insert = 1 and wait_for_async_insert = 0 the async route would return before the flush and lose
# the rows. Every case forces async_insert on and still expects the data present and nothing in
# system.asynchronous_insert_log for this database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Case 1: CREATE MATERIALIZED VIEW ... POPULATE runs under the user session, so async_insert = 1 reaches it.
# A non-refreshable materialized view needs a real source table (it cannot read from a table function),
# so POPULATE copies the source rows into the inner table through the internal INSERT ... SELECT under test.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_internal_populate"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_internal_populate_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_internal_populate_src ENGINE = MergeTree ORDER BY n AS SELECT number AS n FROM numbers(100)"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=0 -q "
    CREATE MATERIALIZED VIEW test_internal_populate
    ENGINE = MergeTree ORDER BY n
    POPULATE
    AS SELECT n FROM test_internal_populate_src
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_internal_populate"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_internal_populate"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_internal_populate_src"

# Case 2: CREATE TABLE ... AS SELECT, also under the user session.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_internal_ctas"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=0 -q "
    CREATE TABLE test_internal_ctas
    ENGINE = MergeTree ORDER BY n
    AS SELECT number AS n FROM numbers(100)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_internal_ctas"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_internal_ctas"

# Case 3: refreshable MV. Its refresh builds a fresh context that ignores the session, so async_insert is
# forced through a dedicated DEFINER user whose profile carries it; getSQLSecurityOverriddenContext loads
# that profile into the refresh context. Without the sync gate the internal insert into the temporary
# inner table would be handed to the queue, then the refresh EXCHANGEs and drops that table before the
# flush, losing the rows.
DEFINER_USER="${CLICKHOUSE_DATABASE}_definer"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_internal_refresh"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEFINER_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${DEFINER_USER} IDENTIFIED WITH no_password SETTINGS async_insert = 1, wait_for_async_insert = 0"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${DEFINER_USER}"
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_internal_refresh
    REFRESH EVERY 1 YEAR
    ENGINE = MergeTree ORDER BY n
    DEFINER = ${DEFINER_USER} SQL SECURITY DEFINER
    AS SELECT number AS n FROM numbers(100)
"
${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH VIEW test_internal_refresh"
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT VIEW test_internal_refresh"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_internal_refresh"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_internal_refresh"
${CLICKHOUSE_CLIENT} -q "DROP USER ${DEFINER_USER}"

# None of the three may have gone through the async insert queue: no entry for this database.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
"
