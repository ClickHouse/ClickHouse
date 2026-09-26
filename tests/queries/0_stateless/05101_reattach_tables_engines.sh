#!/usr/bin/env bash
# Tags: long, no-flaky-check, no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# long: a comprehensive regression suite whose cumulative time across flaky-check reruns exceeds the
#       flaky-check budget, though each run is quick.
# no-flaky-check: the flaky check reruns a test 50 times; a single run of this suite already takes tens
#       of seconds on a sanitizer build, so the reruns hit the per-test timeout. The suite is
#       deterministic (it drives every `DETACH`/`ATTACH` itself and is `no-random-detach`), so there is
#       nothing for the flaky check to shake out here.
# no-fasttest: part of the `long` reattach suite; the fast test runs with `--no-long` and never runs it.

# Table engines and database engines the hook has to skip: a `TTL` carrier, engines with no on-disk
# data, `ON CLUSTER`, and the database engines that do not support a non-permanent `DETACH TABLE`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# A `MergeTree` table whose metadata carries any TTL is skipped: the internal `DETACH TABLE ... SYNC`
# cancels selected-but-not-started background TTL merges, and every such cancellation leaks a
# `max_number_of_merges_with_ttl_in_pool` slot until server restart (see the comment in
# `reattachTablesUsedInQuery` and https://github.com/ClickHouse/ClickHouse/pull/111925).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ttl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ttl (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY"
check_if_not_detached "SELECT * FROM t_reattach_ttl FORMAT Null" "t_reattach_ttl"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ttl"

# A table with an Outdated part that no Active part covers is skipped: the `DETACH`/`ATTACH` cycle
# reloads the parts from disk and would resurrect that part as Active. `ALTER TABLE ... DETACH PART`
# leaves such a part behind — the empty covering part it creates is immediately dropped from the
# working set but stays on disk until the asynchronous cleanup.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_uncovered"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_uncovered (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_uncovered VALUES (1)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_uncovered VALUES (2)"
check_if_detached "SELECT * FROM t_reattach_uncovered FORMAT Null" "t_reattach_uncovered"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_reattach_uncovered DETACH PART 'all_1_1_0'"
check_if_not_detached "SELECT * FROM t_reattach_uncovered FORMAT Null" "t_reattach_uncovered"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_uncovered"

# A replacing form validates the new definition before the replacement path touches the existing
# destination: the populating `SELECT` is analyzed by `getTablePropertiesAndNormalizeCreateQuery`
# and an `AS src` source is validated by `setEngine`, so `CREATE OR REPLACE TABLE dst AS SELECT missing_col
# FROM src` (or `... AS view_src`) fails with `dst` untouched — and a source-less form can be rejected
# there as incomplete too (`CREATE OR REPLACE TABLE dst` with no column list). The hook cannot predict
# whether that validation passes, so every replacing destination must stay out of scope — even for a
# statement that goes on to succeed.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_src"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_repl_view"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_repl_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_repl_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE VIEW t_reattach_repl_view AS SELECT a FROM t_reattach_repl_src"

check_fails_kind_without_detach "CREATE OR REPLACE TABLE t_reattach_repl_dst ENGINE = MergeTree ORDER BY a AS SELECT missing_col FROM t_reattach_repl_src" "t_reattach_repl_dst" "UNKNOWN_IDENTIFIER"
check_fails_kind_without_detach "CREATE OR REPLACE TABLE t_reattach_repl_dst AS t_reattach_repl_view" "t_reattach_repl_dst" "INCORRECT_QUERY"
check_fails_kind_without_detach "CREATE OR REPLACE TABLE t_reattach_repl_dst" "t_reattach_repl_dst" "INCORRECT_QUERY"

# The failing statements above must not have replaced or lost the destination.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_repl_dst"

check_if_not_detached "CREATE OR REPLACE TABLE t_reattach_repl_dst ENGINE = MergeTree ORDER BY a AS SELECT a FROM t_reattach_repl_src" "t_reattach_repl_dst"
check_if_not_detached "CREATE OR REPLACE TABLE t_reattach_repl_dst (a UInt64) ENGINE = MergeTree ORDER BY a" "t_reattach_repl_dst"

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_repl_view"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_repl_dst"

# A temporary-table `CREATE` rejected on its syntax alone — a database-qualified temporary
# (`BAD_DATABASE_FOR_TEMPORARY_TABLE`) or a temporary created `ON CLUSTER` (`INCORRECT_QUERY`) — is thrown
# out at the very top of `InterpreterCreateQuery::createTable`, before the populating `SELECT` or the
# `AS src` structure source is ever analyzed, so the hook must not reattach those sources on the way to
# the rejection. The same statement without the rejected clause does read the source and keeps detaching it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_tmp_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_tmp_src (a UInt64) ENGINE = MergeTree ORDER BY a"

check_fails_kind_without_detach "CREATE TEMPORARY TABLE ${CLICKHOUSE_DATABASE}.t_reattach_tmp ENGINE = Memory AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src" "BAD_DATABASE_FOR_TEMPORARY_TABLE"
check_fails_kind_without_detach "CREATE TEMPORARY TABLE t_reattach_tmp ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src" "INCORRECT_QUERY"
# An explicitly forbidden temporary-table engine is rejected by `setEngine` before it analyzes the
# populating `SELECT` too.
check_fails_kind_without_detach "CREATE TEMPORARY TABLE t_reattach_tmp ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_reattach_tmp', '{replica}') ORDER BY tuple() AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src" "INCORRECT_QUERY"

check_if_detached "CREATE TEMPORARY TABLE t_reattach_tmp ENGINE = Memory AS SELECT * FROM t_reattach_tmp_src" "t_reattach_tmp_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_tmp_src"

# An `ON CLUSTER` statement is out of the hook's scope entirely: on the initiator the interpreter
# delegates to `executeDDLQueryOnCluster` before performing any local table operation (the local host may
# not even be in the target cluster), and the real per-host executions replayed by the `DDLWorker` are not
# `INITIAL_QUERY`, so neither side may reattach. The same statement without the `ON CLUSTER` clause does
# touch the local table and keeps detaching it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_oc"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_oc (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_oc VALUES (1)"
check_if_not_detached "OPTIMIZE TABLE t_reattach_oc ON CLUSTER test_shard_localhost FINAL" "t_reattach_oc"
check_if_detached "OPTIMIZE TABLE t_reattach_oc FINAL" "t_reattach_oc"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_oc"

# Tables of a `URL` database are resolved dynamically, and the resolution itself is not free of side
# effects: it infers the table structure from the data, and for a `file://` URL it requires the read
# source grant already in `tryGetTable` (see `DatabaseURL::getTableImpl`). The hook must reject the
# database (it does not support detaching tables) before resolving any table of it: `EXISTS TABLE`
# requires only `SHOW TABLES`, so for a user without the read source grant the hook's eligibility
# probe would otherwise fail the query with `ACCESS_DENIED`.
URL_DB="db_reattach_url_${CLICKHOUSE_DATABASE}"
URL_USER="user_reattach_url_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${URL_DB}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${URL_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${URL_DB} ENGINE = URL('file://')"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${URL_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${URL_DB}.* TO ${URL_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${URL_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "EXISTS TABLE ${URL_DB}.\`${CLICKHOUSE_USER_FILES_UNIQUE}/02461_data.csv\`" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE"; then
    echo "FAIL (a URL database table was detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP USER ${URL_USER}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${URL_DB}"

# The engine of an engine-less `CREATE ... AS SELECT` is inferred from `default_table_engine`
# (`default_temporary_table_engine` for a temporary table) by `setEngine`, and
# `getTablePropertiesAndNormalizeCreateQuery` then checks the `TABLE ENGINE` grant on the inferred
# engine before the populating `SELECT` is analyzed — exactly as it does for an explicit engine.
# A user who may create the destination but lacks that grant is stopped there, so the source must
# stay attached on the way to the `ACCESS_DENIED`. With the grant in place, the same statement over
# a free destination name no longer stops and must still detach its source.
IMPLICIT_USER="user_reattach_implicit_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${IMPLICIT_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_src"

# The source is a `Log` table so that the user's missing `TABLE ENGINE ON MergeTree` / `ON Memory`
# grant affects only the inferred destination engine: the hook's own reattach preflight on the source
# requires the grant for the source's engine, which the user has.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_implicit_src (a UInt64) ENGINE = Log"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_implicit_src VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${IMPLICIT_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${IMPLICIT_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${IMPLICIT_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON Log TO ${IMPLICIT_USER}"

function check_implicit_engine_denied()
{
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${IMPLICIT_USER}" \
        --reattach_tables_before_query_execution=1 \
        --default_table_engine=MergeTree --default_temporary_table_engine=Memory \
        --query "$1" 2>&1)
    REATTACH_STATUS=$?
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_implicit_src"; then
        echo "FAIL (source detached for an engine-rejected query)"
    else
        echo "OK"
    fi
}

check_implicit_engine_denied "CREATE TABLE t_reattach_implicit_dst AS SELECT * FROM t_reattach_implicit_src"
check_implicit_engine_denied "CREATE TEMPORARY TABLE t_reattach_implicit_tmp AS SELECT * FROM t_reattach_implicit_src"

${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${IMPLICIT_USER}"
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${IMPLICIT_USER}" \
    --reattach_tables_before_query_execution=1 \
    --default_table_engine=MergeTree --create_table_empty_primary_key_by_default=1 \
    --query "CREATE TABLE t_reattach_implicit_dst AS SELECT * FROM t_reattach_implicit_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_implicit_src"; then
    echo "OK"
else
    echo "FAIL (source not detached for a granted implicit-engine statement)"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_implicit_src"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${IMPLICIT_USER}"

# Tables of a `Remote` database are read-through proxies of another server's tables: resolving one
# (`isTableExist`, `tryGetTable`) issues remote metadata RPCs under the caller's credentials and
# enforces `SHOW COLUMNS` on the underlying table (see `DatabaseRemote::fetchTableStructure`). The
# hook must reject the database (it does not support detaching tables) before probing any table of
# it: `EXISTS TABLE` requires only `SHOW TABLES` on the proxy database, so for a user without any
# grant on the underlying table the hook's eligibility probe would otherwise fail the query with
# `ACCESS_DENIED` (the query itself answers `0` — the table stays hidden).
REMOTE_DB="db_reattach_remote_${CLICKHOUSE_DATABASE}"
REMOTE_USER="user_reattach_remote_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${REMOTE_DB}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${REMOTE_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_remote_under"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_remote_under (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}', 'default', '')"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${REMOTE_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${REMOTE_DB}.* TO ${REMOTE_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${REMOTE_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "EXISTS TABLE ${REMOTE_DB}.t_reattach_remote_under" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (the hook probed the underlying table on behalf of the query)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE"; then
    echo "FAIL (a Remote database table was detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP USER ${REMOTE_USER}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${REMOTE_DB}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_remote_under"
