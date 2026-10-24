#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# `joinGet` and the `dictGet` family name an object in their first argument, and the function checks the
# access to that object only when it is built - after the reattach hook. The hook must fold those objects
# into its access preflight, so that a query rejected on them stays side-effect free.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

ACC_USER="user_reattach_fn_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${ACC_USER};
    DROP DICTIONARY IF EXISTS d_reattach_fn;
    DROP TABLE IF EXISTS t_reattach_fn_src;
    DROP TABLE IF EXISTS t_reattach_fn_join;
    DROP TABLE IF EXISTS t_reattach_fn_dict_src;
    CREATE TABLE t_reattach_fn_src (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_reattach_fn_src VALUES (1);
    CREATE TABLE t_reattach_fn_join (a UInt64, v UInt64) ENGINE = Join(ANY, LEFT, a);
    INSERT INTO t_reattach_fn_join VALUES (1, 1);
    CREATE TABLE t_reattach_fn_dict_src (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_reattach_fn_dict_src VALUES (1, 1);
    CREATE DICTIONARY d_reattach_fn (a UInt64, v UInt64) PRIMARY KEY a
        SOURCE(CLICKHOUSE(TABLE 't_reattach_fn_dict_src' DB '${CLICKHOUSE_DATABASE}'))
        LAYOUT(FLAT()) LIFETIME(0);
    CREATE USER ${ACC_USER} IDENTIFIED WITH no_password;
    GRANT TABLE ENGINE ON MergeTree TO ${ACC_USER};
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_fn_src TO ${ACC_USER};
"

# The user may read and reattach `t_reattach_fn_src`, but lacks the access the function checks on the
# object it names: the query fails with `ACCESS_DENIED`, and `t_reattach_fn_src` must not be detached.
# Parallel replicas are disabled: with them, the function may not be built under this user at all.
function check_access_rejected()
{
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ACC_USER}" \
        --enable_parallel_replicas=0 \
        --reattach_tables_before_query_execution=1 \
        --query "$1" 2>&1)
    REATTACH_STATUS=$?
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_fn_src"; then
        echo "FAIL (a table was detached for an access-rejected query)"
    else
        echo "OK"
    fi
}

echo "access rejected"
check_access_rejected "SELECT count() FROM t_reattach_fn_src WHERE joinGet(t_reattach_fn_join, 'v', a) = 1"
check_access_rejected "SELECT count() FROM t_reattach_fn_src WHERE joinGet('t_reattach_fn_join', 'v', a) = 1"
check_access_rejected "SELECT count() FROM t_reattach_fn_src WHERE joinGetOrNull('${CLICKHOUSE_DATABASE}.t_reattach_fn_join', 'v', a) = 1"
check_access_rejected "SELECT count() FROM t_reattach_fn_src WHERE dictGet(d_reattach_fn, 'v', a) = 1"
check_access_rejected "SELECT count() FROM t_reattach_fn_src WHERE dictGet('d_reattach_fn', 'v', a) = 1"
check_access_rejected "SELECT count() FROM t_reattach_fn_src WHERE dictHas('${CLICKHOUSE_DATABASE}.d_reattach_fn', a)"

# A table function checks its access only when it is executed, so a query reading one in `FROM`/`JOIN`
# cannot be preflighted and must be skipped, both when it is rejected and when it succeeds.
echo "table function"
check_access_rejected "SELECT count() FROM t_reattach_fn_src AS s JOIN dictionary('${CLICKHOUSE_DATABASE}.d_reattach_fn') AS d ON s.a = d.a"
check_if_not_detached "SELECT count() FROM t_reattach_fn_src AS s JOIN dictionary('${CLICKHOUSE_DATABASE}.d_reattach_fn') AS d ON s.a = d.a" t_reattach_fn_src
check_if_not_detached "SELECT count() FROM t_reattach_fn_src, numbers(1) AS n" t_reattach_fn_src

# With the access in place, the `Join` table `joinGet` reads is reattached like any other table it reads.
echo "join table reattached"
check_if_detached "SELECT joinGet('t_reattach_fn_join', 'v', toUInt64(1))" t_reattach_fn_join
check_if_detached "SELECT joinGet(t_reattach_fn_join, 'v', toUInt64(1))" t_reattach_fn_join

# A first argument bound to an alias names an object that is not known before the analysis, so the hook
# cannot preflight it and must skip the query, including the `Join` table the name would resolve to.
echo "unverifiable name"
check_if_not_detached "WITH 't_reattach_fn_join' AS name SELECT count() FROM t_reattach_fn_src WHERE joinGet(name, 'v', a) = 1" t_reattach_fn_src
check_last_query_did_not_detach t_reattach_fn_join
check_if_not_detached "WITH 'd_reattach_fn' AS name SELECT count() FROM t_reattach_fn_src WHERE dictGet(name, 'v', a) = 1" t_reattach_fn_src

# `CREATE FUNCTION` only validates and stores its body, so a table the body names is never read by the
# statement and must not be reattached - neither when the statement succeeds, nor when it is rejected
# because the user, who may read and reattach that table, lacks `CREATE FUNCTION`.
echo "stored function body"
FN_NAME="f_reattach_fn_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_fn_join TO ${ACC_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON Join TO ${ACC_USER}"
REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ACC_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "CREATE FUNCTION ${FN_NAME} AS x -> joinGet('${CLICKHOUSE_DATABASE}.t_reattach_fn_join', 'v', x)" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
else
    check_last_query_did_not_detach t_reattach_fn_join
fi
check_if_not_detached "CREATE FUNCTION ${FN_NAME} AS x -> joinGet('${CLICKHOUSE_DATABASE}.t_reattach_fn_join', 'v', x)" t_reattach_fn_join
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_NAME}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${ACC_USER};
    DROP DICTIONARY IF EXISTS d_reattach_fn;
    DROP TABLE IF EXISTS t_reattach_fn_src;
    DROP TABLE IF EXISTS t_reattach_fn_join;
    DROP TABLE IF EXISTS t_reattach_fn_dict_src;
"
