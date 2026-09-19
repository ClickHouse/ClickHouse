#!/usr/bin/env bash
# Tags: long, no-flaky-check, no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# long: a comprehensive regression suite whose cumulative time across flaky-check reruns exceeds the
#       flaky-check budget, though each run is quick.
# no-flaky-check: the flaky check reruns a test 50 times; a single run of this suite already takes tens
#       of seconds on a sanitizer build, so the reruns hit the per-test timeout. The suite is
#       deterministic (it drives every `DETACH`/`ATTACH` itself and is `no-random-detach`), so there is
#       nothing for the flaky check to shake out here.
# no-fasttest: the `DELETE ... IN PARTITION` rejection needs a `supportsDelete` target, and the only one
#              available in a stateless test is `EmbeddedRocksDB`, which the fast test does not build
#              (`ENABLE_LIBRARIES=0`). The fast test runs with `--no-long` and already skips this suite.

# The mutation and partition carriers, a table another query is holding, the per-database `max_tables`
# limit the internal `ATTACH` back has to fit into, and `INSERT`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# The mutation/partition carriers must stop before their sources when the target itself rejects the
# operation: `InterpreterDeleteQuery` / `InterpreterUpdateQuery` / `InterpreterAlterQuery` fast-fail on
# the target engine (`supportsDelete`, `supportsLightweightUpdate`, `checkMutationIsPossible`,
# `checkAlterPartitionIsPossible`) before the tables named by the predicate, the update expressions, or
# the partition `FROM`/`TO TABLE` clause are ever read, so the hook must not `DETACH`/`ATTACH` those
# sources on the way to the rejection (see `mutationQueryStopsBeforeSources`).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_log (a UInt64) ENGINE = Log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_src (a UInt64) ENGINE = MergeTree ORDER BY a"

function check_source_not_detached_for_failing_mutation()
{
    query="$1"
    expected_error="$2"
    check_if_detached_impl "$query" "t_reattach_mut_src"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "$expected_error"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_mut_src"; then
        echo "FAIL (source detached for a query failing on its target)"
    else
        echo "OK"
    fi
}

check_source_not_detached_for_failing_mutation "DELETE FROM t_reattach_mut_log WHERE a IN (SELECT a FROM t_reattach_mut_src)" "BAD_ARGUMENTS"
check_source_not_detached_for_failing_mutation "UPDATE t_reattach_mut_log SET a = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_log DELETE WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_log UPDATE a = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_log REPLACE PARTITION ID 'all' FROM t_reattach_mut_src" "NOT_IMPLEMENTED"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_log MOVE PARTITION ID 'all' TO TABLE t_reattach_mut_src" "NOT_IMPLEMENTED"

# Positive controls: the same statement shapes on a supporting (`MergeTree`) target succeed and do
# randomize their sources. Mutations are synchronous here so a background mutation cannot outlive the
# source table it reads.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_mt"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_mt (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_mut_mt VALUES (1)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_mut_src VALUES (2)"

check_if_detached "DELETE FROM t_reattach_mut_mt WHERE a IN (SELECT a FROM t_reattach_mut_src)" "t_reattach_mut_src"
check_if_detached "ALTER TABLE t_reattach_mut_mt DELETE WHERE a IN (SELECT a FROM t_reattach_mut_src) SETTINGS mutations_sync = 1" "t_reattach_mut_src"
check_if_detached "ALTER TABLE t_reattach_mut_mt REPLACE PARTITION ID 'all' FROM t_reattach_mut_src" "t_reattach_mut_src"

# A `supportsDelete` target takes the statement as a mutation carrying only the serialized predicate, and
# those storages have no notion of `MergeTree`-style partitions, so `InterpreterDeleteQuery` rejects an
# `IN PARTITION` clause with `NOT_IMPLEMENTED` instead of mutating a wider scope than the statement asked
# for. That happens before the command is built and before the mutation probes, hence before the predicate's
# tables are read, so the source must not be randomized on the way to the rejection — in the single-partition
# shape (`ASTDeleteQuery::partition`) as well as the list one (`ASTDeleteQuery::partitions`).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_kv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_kv (a UInt64, v UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_mut_kv VALUES (2, 20)"

check_source_not_detached_for_failing_mutation "DELETE FROM t_reattach_mut_kv IN PARTITION ID 'all' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"
check_source_not_detached_for_failing_mutation "DELETE FROM t_reattach_mut_kv IN PARTITION ID 'all', ID 'other' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"

# The `ALTER` carrier reaches the same clause through `getPartitionAndPredicateExpressionForMutationCommand`,
# which `MutationsInterpreter::prepare` calls before it resolves the commands' predicates and expressions:
# a target outside the `MergeTree` family rejects the clause there with `NOT_IMPLEMENTED`, again before the
# predicate's tables are read.
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_kv DELETE IN PARTITION ID 'all' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_kv UPDATE v = 1 IN PARTITION ID 'all', ID 'other' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NOT_IMPLEMENTED"

# Without the clause the same target accepts the statement and its mutation does read the predicate's table,
# so that source must still be randomized: the skip above is about the rejected shape only.
check_if_detached "DELETE FROM t_reattach_mut_kv WHERE a IN (SELECT a FROM t_reattach_mut_src)" "t_reattach_mut_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_mut_kv"

# That same resolution rejects a clause that does not match a `MergeTree` target's partition key
# (`getPartitionIDFromQuery`), on the lightweight `DELETE` carrier as well as on the `ALTER` one, and a
# rejection there also precedes the predicate's tables. A clause that does match is resolved fine, so the
# statement goes on to read the source and must randomize it.
check_source_not_detached_for_failing_mutation "DELETE FROM t_reattach_mut_mt IN PARTITION ID 'no_such' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "INVALID_PARTITION_VALUE"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_mt DELETE IN PARTITION tuple(1, 2, 3) WHERE a IN (SELECT a FROM t_reattach_mut_src)" "INVALID_PARTITION_VALUE"
check_if_detached "ALTER TABLE t_reattach_mut_mt DELETE IN PARTITION ID 'all' WHERE a IN (SELECT a FROM t_reattach_mut_src) SETTINGS mutations_sync = 1" "t_reattach_mut_src"

# A lightweight `UPDATE` throws on `enable_lightweight_update = 0` as the very first thing its interpreter
# does, and SQL UDF substitution runs before the predicate's tables are read in both the `UPDATE` and the
# `ALTER` carriers. Neither rejection may detach the source.
check_source_not_detached_for_failing_mutation "UPDATE t_reattach_mut_mt SET a = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src) SETTINGS enable_lightweight_update = 0" "SUPPORT_IS_DISABLED"

# `MutationsInterpreter::prepare` validates the updated columns against the TARGET's metadata before it
# resolves the predicate's and the assignments' subqueries, so an update it rejects — of a key column, or of
# a column the target does not have — never reads the tables those expressions name. That gate sits on the
# lightweight `UPDATE` carrier and on both the lightweight and the heavy `ALTER ... UPDATE` form, and none of
# them may detach the source on the way to the rejection.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_lw"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_lw (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_mut_lw VALUES (1, 1)"

check_source_not_detached_for_failing_mutation "UPDATE t_reattach_mut_lw SET a = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)" "CANNOT_UPDATE_COLUMN"
check_source_not_detached_for_failing_mutation "UPDATE t_reattach_mut_lw SET no_such_column = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)" "NO_SUCH_COLUMN_IN_TABLE"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_lw UPDATE a = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src) SETTINGS alter_update_mode = 'lightweight'" "CANNOT_UPDATE_COLUMN"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_lw UPDATE a = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)" "CANNOT_UPDATE_COLUMN"

# Updating an ordinary non-key column instead passes that validation, so the statement does read the source
# and must randomize it — on the lightweight carrier and on the heavy `ALTER` one.
check_if_detached "UPDATE t_reattach_mut_lw SET b = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)" "t_reattach_mut_src"
check_if_detached "ALTER TABLE t_reattach_mut_lw UPDATE b = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src) SETTINGS mutations_sync = 1" "t_reattach_mut_src"

# The `UPDATE` carrier resolves an `IN PARTITION` clause against the target through the same
# `getPartitionAndPredicateExpressionForMutationCommand`, so a clause that does not match the partition key
# is rejected before the predicate's tables are read, while a matching one lets the statement reach them.
check_source_not_detached_for_failing_mutation "UPDATE t_reattach_mut_lw SET b = 1 IN PARTITION ID 'no_such' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "INVALID_PARTITION_VALUE"
check_if_detached "UPDATE t_reattach_mut_lw SET b = 1 IN PARTITION ID 'all' WHERE a IN (SELECT a FROM t_reattach_mut_src)" "t_reattach_mut_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_lw"

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS reattach_recursive_mut_udf"
${CLICKHOUSE_CLIENT} -q "CREATE FUNCTION reattach_recursive_mut_udf AS x -> reattach_recursive_mut_udf(x)"
check_source_not_detached_for_failing_mutation "UPDATE t_reattach_mut_mt SET a = 1 WHERE reattach_recursive_mut_udf(a) IN (SELECT a FROM t_reattach_mut_src)" "UNSUPPORTED_METHOD"
check_source_not_detached_for_failing_mutation "ALTER TABLE t_reattach_mut_mt UPDATE a = reattach_recursive_mut_udf(a) WHERE a IN (SELECT a FROM t_reattach_mut_src)" "UNSUPPORTED_METHOD"
check_source_not_detached_for_failing_mutation "DELETE FROM t_reattach_mut_mt WHERE reattach_recursive_mut_udf(a) IN (SELECT a FROM t_reattach_mut_src)" "UNSUPPORTED_METHOD"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION reattach_recursive_mut_udf"

# An unqualified table inside a mutation's predicate is resolved against the *target table's* database
# (`AddDefaultDatabaseVisitor` in `InterpreterUpdateQuery` / `InterpreterAlterQuery`), not against the
# session's current database. So the same-named table in the target's database is the one the statement
# reads and randomizes, while the same-named table in the current database is untouched.
MUT_DB="${CLICKHOUSE_DATABASE}_mut"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${MUT_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${MUT_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${MUT_DB}.t_reattach_mut_dst (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${MUT_DB}.t_reattach_mut_other (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_other (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_detached_impl "ALTER TABLE ${MUT_DB}.t_reattach_mut_dst DELETE WHERE a IN (SELECT a FROM t_reattach_mut_other) SETTINGS mutations_sync = 1" "t_reattach_mut_other"
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_mut_other"; then
    echo "FAIL (a table of the session database detached for a mutation reading the target's database)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE ${MUT_DB}.t_reattach_mut_other"; then
    echo "FAIL (the source in the target's database was not detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_mut_other"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${MUT_DB}"

# `InterpreterAlterQuery::executeToTable` resolves an unqualified `ALTER` target with the default
# `ResolveAll`, so a session temporary table shadows the persistent one, while `InterpreterUpdateQuery`
# and `InterpreterDeleteQuery` resolve theirs with an explicit `ResolveOrdinary` and are never shadowed.
# The mutation preflight has to follow the same namespace: for a shadowed `ALTER` the statement qualifies
# the tables of its predicate with the temporary target's database (`_temporary_and_external_tables`) and
# dies on that, so the same-named persistent table in the session's database is never read and must not be
# detached — which is what would happen if the preflight probed the persistent target instead.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_shadow"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_mut_shadow (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a"

function check_source_not_detached_for_shadowed_alter()
{
    query="$1"
    check_if_detached_impl "CREATE TEMPORARY TABLE t_reattach_mut_shadow (a UInt64, b UInt64) ENGINE = Memory; $query" "t_reattach_mut_src"
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "DATABASE_ACCESS_DENIED"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_mut_src"; then
        echo "FAIL (source detached for an ALTER hitting its shadowing temporary target)"
    else
        echo "OK"
    fi
}

check_source_not_detached_for_shadowed_alter "ALTER TABLE t_reattach_mut_shadow DELETE WHERE a IN (SELECT a FROM t_reattach_mut_src)"
check_source_not_detached_for_shadowed_alter "ALTER TABLE t_reattach_mut_shadow UPDATE b = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src)"

# The same shapes without the shadowing temporary table do reach and randomize the source.
check_if_detached "ALTER TABLE t_reattach_mut_shadow UPDATE b = 1 WHERE a IN (SELECT a FROM t_reattach_mut_src) SETTINGS mutations_sync = 1" "t_reattach_mut_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_mut_shadow"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_mt"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_mut_src"

# A table another query is using right now must NOT be reattached. The internal `DETACH TABLE ... SYNC`
# removes the table from its database and then waits, with no deadline, for the detached storage to stop
# being referenced, so the hook would hold this query for as long as the concurrent one runs — and the
# script cannot end that one, because the session that would is the blocked one. The hook probes the
# table's exclusive lock and skips a table that is in use.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_busy"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_busy (a UInt64) ENGINE = MergeTree ORDER BY a"

BUSY_QUERY_ID="reattach_busy_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$BUSY_QUERY_ID" --function_sleep_max_microseconds_per_block 60000000 \
    -q "INSERT INTO t_reattach_busy SELECT sleep(1) FROM numbers(30) SETTINGS max_block_size = 1" > /dev/null 2>&1 &
busy_pid=$!

# Wait for the holder to be running rather than for a fixed time: whether it is still holding the table
# when the hook probes is what the check below is about, and a slow runner must not decide it.
for _ in {0..600}
do
    if [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$BUSY_QUERY_ID'")" -gt 0 ]]
    then
        break
    fi
    sleep 0.1
done

check_if_not_detached "SELECT count() FROM t_reattach_busy" "t_reattach_busy"

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$BUSY_QUERY_ID' SYNC" > /dev/null 2>&1
wait "$busy_pid" 2>/dev/null

# The same table is reattached again once nobody holds it.
check_if_detached "SELECT count() FROM t_reattach_busy" "t_reattach_busy"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_busy"

# A database may hold more tables than its own `max_tables` limit allows: `ALTER DATABASE ... MODIFY
# SETTING max_tables` lowers the limit without detaching anything. The internal `ATTACH TABLE` of the
# reattach cycle is subject to that limit exactly as a `CREATE` is, so reattaching a table of such a
# database would fail with `TOO_MANY_TABLES` and leave the table detached — failing an outer query that
# the limit does not concern at all. The hook must skip such a table, and the query must succeed.
LIMIT_DB="${CLICKHOUSE_DATABASE}_max_tables"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${LIMIT_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${LIMIT_DB} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${LIMIT_DB}.t_reattach_limit_1 (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${LIMIT_DB}.t_reattach_limit_2 (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "ALTER DATABASE ${LIMIT_DB} MODIFY SETTING max_tables = 1"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT count() FROM ${LIMIT_DB}.t_reattach_limit_1" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE ${LIMIT_DB}.t_reattach_limit_1"; then
    echo "FAIL (a table of a database over its max_tables limit was detached)"
else
    echo "OK"
fi

# A database sitting exactly at its limit is not over it: the `ATTACH` back happens while this table is
# already detached, so the slot it frees is the one it takes again. Such a table stays a candidate.
${CLICKHOUSE_CLIENT} -q "ALTER DATABASE ${LIMIT_DB} MODIFY SETTING max_tables = 2"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT count() FROM ${LIMIT_DB}.t_reattach_limit_1" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE ${LIMIT_DB}.t_reattach_limit_1"; then
    echo "OK"
else
    echo "FAIL (table not detached although its database is within its max_tables limit)"
fi

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${LIMIT_DB}"

# `InterpreterInsertQuery::execute` validates the destination — resolving it, the insertion prohibitions,
# the `PARTITION BY` support, the header derived from the statement's column list, `checkInsertIsAllowed` —
# before it builds the pipeline that reads the tables the `SELECT` names. An insert that is still going to
# be rejected on its destination alone therefore never reads its sources, and must not detach them. Its
# destination, on the other hand, is resolved, locked and read even by the rejected statement, so that one
# stays a reattach candidate — which also proves the hook ran at all for this query.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ins_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ins_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ins_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ins_dst (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_detached_impl "INSERT INTO t_reattach_ins_dst (no_such_column) SELECT * FROM t_reattach_ins_src" "t_reattach_ins_dst"
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (insert into a column the destination does not have unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "NO_SUCH_COLUMN_IN_TABLE"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_ins_src"; then
    echo "FAIL (source detached for an insert that fails on its destination)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_ins_dst"; then
    echo "FAIL (destination not detached although the insert reaches it)"
else
    echo "OK"
fi

# An insert whose column list the destination accepts does read its sources, so both are detached.
check_if_detached "INSERT INTO t_reattach_ins_dst (a) SELECT a FROM t_reattach_ins_src" "t_reattach_ins_src"
check_if_detached "INSERT INTO t_reattach_ins_dst (a) SELECT a FROM t_reattach_ins_src" "t_reattach_ins_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ins_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ins_dst"
