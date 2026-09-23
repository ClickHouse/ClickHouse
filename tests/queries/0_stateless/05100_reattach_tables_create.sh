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

# References that cannot be resolved, metadata probes for the wrong object kind, temporary tables
# shadowing a persistent name, and the `CREATE` destinations that stop a statement before its sources.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# A required table reference that does not exist means the query itself is going to fail (with
# UNKNOWN_TABLE, UNKNOWN_IDENTIFIER under the analyzer for the `IN` form, or CANNOT_GET_CREATE_TABLE_QUERY
# for the `CREATE ... AS src` form), so the hook must skip entirely: the references that do exist must NOT
# be detached first. Covers FROM/JOIN, the `IN table` form, and the `CREATE ... AS src` string field.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_unres (a UInt64) ENGINE = MergeTree ORDER BY a"

function check_fails_without_detach()
{
    check_if_detached_impl "$1" "$2"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q -e "UNKNOWN_TABLE" -e "UNKNOWN_IDENTIFIER" -e "CANNOT_GET_CREATE_TABLE_QUERY"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "FAIL (table was detached for a query referencing a missing table)"
    else
        echo "OK"
    fi
}

check_fails_without_detach "SELECT * FROM t_reattach_unres JOIN t_reattach_unres_missing USING a" "t_reattach_unres"
check_fails_without_detach "SELECT * FROM t_reattach_unres WHERE a IN t_reattach_unres_missing" "t_reattach_unres"
# The same, with a child-scope alias taking the missing table's name. The outer `IN` cannot resolve that
# alias, so the query still fails on the missing table — and the hook must see the reference, otherwise it
# detaches and re-attaches the FROM table for a query that never runs.
check_fails_without_detach "SELECT * FROM t_reattach_unres WHERE a IN t_reattach_unres_missing AND 1 = (SELECT 1 AS t_reattach_unres_missing)" "t_reattach_unres"
check_fails_without_detach "CREATE OR REPLACE TABLE t_reattach_unres AS t_reattach_unres_missing" "t_reattach_unres"

# An OPTIONAL miss must not disable the hook: the target of a plain `CREATE ... AS src` does not exist yet
# (that is the point of the query), and the resolvable source is still detached.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres_new"
check_if_detached "CREATE TABLE t_reattach_unres_new AS t_reattach_unres" "t_reattach_unres"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres_new"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_unres"

# A kind-specific metadata probe on a name that resolves to a plain table never touches that table's
# storage: `EXISTS VIEW` / `EXISTS DICTIONARY` answer 0, and `SHOW CREATE VIEW` / `SHOW CREATE DICTIONARY`
# fail with BAD_ARGUMENTS ("... is not a VIEW" / "... is not a DICTIONARY"). The reattach hook must NOT
# detach the unrelated table in either case.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_kind"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_kind (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_not_detached "EXISTS VIEW t_reattach_kind" "t_reattach_kind"
check_if_not_detached "EXISTS DICTIONARY t_reattach_kind" "t_reattach_kind"

function check_fails_after_detaching()
{
    local expected_error="$3"
    check_if_detached_impl "$1" "$2"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "$expected_error"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$2"; then
        echo "OK"
    else
        echo "FAIL (source was not detached before the query failed)"
    fi
}

check_fails_kind_without_detach "SHOW CREATE VIEW t_reattach_kind" "t_reattach_kind"
check_fails_kind_without_detach "SHOW CREATE DICTIONARY t_reattach_kind" "t_reattach_kind"

# The kind-specific `DROP`/`DETACH` forms fail the same way: `InterpreterDropQuery` throws INCORRECT_QUERY
# on an `is_view`/`is_dictionary` mismatch before touching the table's storage, so the hook must not
# detach the table either.
check_fails_kind_without_detach "DROP VIEW t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"
check_fails_kind_without_detach "DETACH VIEW t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"
check_fails_kind_without_detach "DROP DICTIONARY t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"
check_fails_kind_without_detach "DETACH DICTIONARY t_reattach_kind" "t_reattach_kind" "INCORRECT_QUERY"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_kind"

# A session temporary table with the same name as a persistent one must affect the hook exactly as it
# affects the query itself. Carriers whose interpreter resolves temporary tables first (SELECT,
# SHOW CREATE TABLE) target the temporary table, so the persistent one must NOT be detached. Carriers
# whose interpreter looks the name up only in the persistent catalog (EXISTS TABLE, CREATE ... AS src)
# use the persistent table, so it must still be detached — the temporary hit must not hide it from the
# collector.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_shadow (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_not_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); SELECT * FROM t_reattach_shadow" "t_reattach_shadow"
check_if_not_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); SHOW CREATE TABLE t_reattach_shadow FORMAT Null" "t_reattach_shadow"
check_if_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); EXISTS TABLE t_reattach_shadow" "t_reattach_shadow"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow_new"
check_if_detached "CREATE TEMPORARY TABLE t_reattach_shadow (a UInt64); CREATE TABLE t_reattach_shadow_new AS t_reattach_shadow" "t_reattach_shadow"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow_new"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_shadow"

# Index-management statements travel through `ASTQueryWithTableAndOutput` like `ALTER TABLE`, but not all of
# them reach the table. `InterpreterCreateIndexQuery` rewrites `CREATE INDEX` to `ALTER TABLE ... ADD INDEX`
# only after `validateCreateIndexQuery` accepts it: `CREATE UNIQUE INDEX` throws unless
# `create_index_ignore_unique` is set, and `CREATE INDEX` without a `TYPE` either throws or (with
# `allow_create_index_without_type`) is a no-op. Those shapes must NOT detach the table, while the shapes
# that really rewrite — and `DROP INDEX`, which always rewrites — must.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_index"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_index (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a"

check_fails_kind_without_detach "CREATE UNIQUE INDEX idx_u ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index" "NOT_IMPLEMENTED"
check_fails_kind_without_detach "CREATE INDEX idx_no_type ON t_reattach_index (b)" "t_reattach_index" "INCORRECT_QUERY"
check_if_not_detached "SET allow_create_index_without_type = 1; CREATE INDEX idx_no_type ON t_reattach_index (b)" "t_reattach_index"

check_if_detached "SET create_index_ignore_unique = 1; CREATE UNIQUE INDEX idx_u ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index"
check_if_detached "CREATE INDEX idx_t ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index"
check_if_detached "DROP INDEX idx_t ON t_reattach_index" "t_reattach_index"

# `CREATE`/`DROP HYPOTHETICAL INDEX` and `CREATE`/`DROP HYPOTHETICAL PROJECTION` never mutate the table:
# the interpreter only reads its metadata and updates the session-local hypothetical-object store, so the
# hook must not detach the table for them.
check_if_not_detached "CREATE HYPOTHETICAL INDEX idx_h ON t_reattach_index (b) TYPE minmax GRANULARITY 1" "t_reattach_index"
check_if_not_detached "DROP HYPOTHETICAL INDEX IF EXISTS idx_h ON t_reattach_index" "t_reattach_index"
check_if_not_detached "CREATE HYPOTHETICAL PROJECTION proj_h ON t_reattach_index (SELECT a, b ORDER BY b)" "t_reattach_index"
check_if_not_detached "DROP HYPOTHETICAL PROJECTION IF EXISTS proj_h ON t_reattach_index" "t_reattach_index"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_index"

# A `CREATE` statement stops on its own destination before it ever reads the tables it selects from:
# `InterpreterCreateQuery::execute` checks the destination-side access first, and the plain-create path
# then short-circuits on a taken destination name. Neither shape may detach the source.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_dest_src (a UInt64) ENGINE = MergeTree ORDER BY a"

# 1. Destination access. The user has full grants on the source (so it is a genuine detach candidate) but
# no `CREATE VIEW` on the destination, so `CREATE VIEW v AS SELECT * FROM src` fails with `ACCESS_DENIED`.
DEST_USER="user_reattach_dest_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEST_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${DEST_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.t_reattach_dest_src TO ${DEST_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${DEST_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE VIEW t_reattach_dest_view AS SELECT * FROM t_reattach_dest_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_dest_src"; then
    echo "FAIL (source detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_dest_view"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEST_USER}"

# 2. SQL-security and definer validation follows the destination-access check but precedes reading the
# source of a view. A nonexistent `DEFINER` therefore rejects the statement before it reaches `src`, so
# the hook must keep `src` attached.
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE VIEW t_reattach_dest_view DEFINER = missing_reattach_definer SQL SECURITY DEFINER AS SELECT * FROM t_reattach_dest_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "UNKNOWN_USER"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_dest_src"; then
    echo "FAIL (source detached for a SQL-security-rejected query)"
else
    echo "OK"
fi

# A refreshable materialized view synthesizes an empty `SQL SECURITY` clause before validating it. The
# default `SQL SECURITY NONE` then fails for a user without `ALLOW SQL SECURITY NONE`, before the view
# reads its source; the hook must mirror that synthesized clause and keep the source attached.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_refresh_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_dest_refresh_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEST_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${DEST_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${DEST_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${DEST_USER}" \
    --reattach_tables_before_query_execution=1 \
    --default_materialized_view_sql_security=NONE \
    --query "CREATE MATERIALIZED VIEW t_reattach_dest_refresh REFRESH EVERY 1 HOUR TO t_reattach_dest_refresh_dst AS SELECT * FROM t_reattach_dest_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_dest_src"; then
    echo "FAIL (source detached for a default-SQL-security-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_dest_refresh"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_refresh_dst"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DEST_USER}"

# 3. Taken destination name. A source-carrying `CREATE` resolves its `AS` source or analyzes its
# populating `SELECT` before checking the destination, so the source is detached even when the destination
# turns the statement into a no-op or causes `TABLE_ALREADY_EXISTS`.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_taken"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_dest_taken (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_detached "CREATE TABLE IF NOT EXISTS t_reattach_dest_taken ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src"
check_if_detached "CREATE VIEW IF NOT EXISTS t_reattach_dest_taken AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src"
check_fails_after_detaching "CREATE TABLE t_reattach_dest_taken ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src" "TABLE_ALREADY_EXISTS"

# A parameterized view does not analyze its SELECT at creation time, and validation of an ordinary
# view's alias list can fail before it does. Neither form may detach its source.
check_if_not_detached "CREATE VIEW t_reattach_parameterized_view AS SELECT * FROM t_reattach_dest_src WHERE a = {p:UInt64}" "t_reattach_dest_src"
${CLICKHOUSE_CLIENT} -q "DROP VIEW t_reattach_parameterized_view"
check_fails_kind_without_detach "CREATE VIEW t_reattach_alias_view (a) AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src" "BAD_ARGUMENTS"

# Fresh view definitions reject query-construction settings before analyzing their SELECT, so a
# rejected definition must leave its source attached.
check_fails_kind_without_detach "CREATE VIEW t_reattach_construction_settings_view AS SELECT * FROM t_reattach_dest_src SETTINGS limit = 1" "t_reattach_dest_src" "NOT_IMPLEMENTED"

# `ATTACH ... FROM` validates its data path before inspecting an `AS` source, so a rejected path must
# leave that source attached.
check_fails_kind_without_detach "ATTACH TABLE t_reattach_attach_from_dst FROM '/outside' ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src" "PATH_ACCESS_DENIED"

# `ATTACH ... AS [NOT] REPLICATED` is only supported by short `ATTACH` statements. A full definition is
# rejected before its `AS SELECT` source is analyzed, so it must leave the source attached.
check_fails_kind_without_detach "ATTACH TABLE t_reattach_attach_replicated AS REPLICATED (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src" "SUPPORT_IS_DISABLED"

# SQL UDF substitution runs before `CREATE VIEW` reads its source. A recursive UDF makes substitution
# fail with `UNSUPPORTED_METHOD`, so the source must remain attached.
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS reattach_recursive_udf"
${CLICKHOUSE_CLIENT} -q "CREATE FUNCTION reattach_recursive_udf AS x -> reattach_recursive_udf(x)"
check_fails_kind_without_detach "CREATE VIEW t_reattach_udf_view AS SELECT reattach_recursive_udf(a) FROM t_reattach_dest_src" "t_reattach_dest_src" "UNSUPPORTED_METHOD"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION reattach_recursive_udf"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_free"
check_if_detached "CREATE TABLE t_reattach_dest_free ENGINE = MergeTree ORDER BY a AS SELECT * FROM t_reattach_dest_src" "t_reattach_dest_src"

# 3. Stub `ATTACH` with dropped clauses. An `ATTACH` without an engine and a column list applies the table
# definition from stored metadata and rejects any user-supplied clause it would otherwise silently drop
# with `BAD_ARGUMENTS` before reading any source or target table. The materialized-view form
# `ATTACH MATERIALIZED VIEW mv TO dst AS SELECT ... FROM src` is the parseable shape of this rejection
# that names other live tables — both its external `TO` target and its `SELECT` source must stay
# attached on the way to it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_attach_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_attach_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_reattach_attach_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_target"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mv_target (a UInt64) ENGINE = MergeTree ORDER BY a"

check_fails_kind_without_detach "ATTACH MATERIALIZED VIEW t_reattach_attach_dst TO t_reattach_mv_target AS SELECT a FROM t_reattach_dest_src" "t_reattach_mv_target" "BAD_ARGUMENTS"
check_fails_kind_without_detach "ATTACH MATERIALIZED VIEW t_reattach_attach_dst TO t_reattach_mv_target AS SELECT a FROM t_reattach_dest_src" "t_reattach_dest_src" "BAD_ARGUMENTS"

# The rejections must have left no side effects behind: the proper stub `ATTACH` still works.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_reattach_attach_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_attach_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_attach_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mv_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_free"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_taken"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_dest_src"
