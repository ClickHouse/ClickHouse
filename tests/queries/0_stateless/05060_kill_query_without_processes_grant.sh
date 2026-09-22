#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U1="u1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U2="u2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U3="u3_${CLICKHOUSE_TEST_UNIQUE_NAME}"
A1="a1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
A2="a2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $U1, $U2, $U3, $A1, ${A1}_renamed, $A2, ${A2}_new"
$CLICKHOUSE_CLIENT -q "CREATE USER $U1, $U2, $U3 IDENTIFIED WITH no_password"
# Deliberately no SELECT on system.processes for any user under test.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $U1, $U2, $U3"
$CLICKHOUSE_CLIENT -q "GRANT KILL QUERY ON *.* TO $U3"

# $1 = user, $2 = query_id. Every poll loop is bounded so a regression fails instead of hanging.
function start_victim()
{
    $CLICKHOUSE_CLIENT --user "$1" --query_id "$2" -q \
        "SELECT sleep(0.1) FROM numbers(100000) SETTINGS max_block_size = 1, max_rows_to_read = 0" \
        > /dev/null 2>&1 &
    for _ in {1..150}; do
        if [[ "$(running "$2")" == "1" ]]; then
            return
        fi
        sleep 0.2
    done
    echo "victim $2 never appeared in system.processes"
}

# Observed with the default user, which holds the grants the users under test lack.
function running()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$1'"
}

function wait_gone()
{
    for _ in {1..150}; do
        if [[ "$(running "$1")" == "0" ]]; then
            echo "gone"
            return
        fi
        sleep 0.2
    done
    echo "still running"
}

function drop_victim()
{
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$1' ASYNC" > /dev/null 2>&1
}

# The denial is reported both by the client and by the echoed server log, so match, do not count.
function denial()
{
    if grep -q -F "ACCESS_DENIED" <<< "$1"; then
        echo "ACCESS_DENIED"
    else
        echo "no error"
    fi
}

# A caller who is refused must be refused for want of the grant, not answered with an empty result.
function processes_grant_denial()
{
    if grep -q -F "the grant SELECT ON system.processes" <<< "$1"; then
        echo "needs SELECT ON system.processes"
    else
        echo "unexpected: $1"
    fi
}

echo "-- 1. own query by id, holding neither grant"
start_victim "$U1" "own_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "own_$ID")"
drop_victim "own_$ID"

echo "-- 2. another user's id: no rows, no error, victim untouched"
start_victim "$U2" "other_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'other_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "victim: $(running "other_$ID")"
drop_victim "other_$ID"

echo "-- 3. an id nobody is running: no rows, no error"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'absent_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"

echo "-- 4. every condition other than the one matched shape behaves as before"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE user = currentUser() ASYNC" 2>&1)
denial "$OUT"
start_victim "$U1" "conj_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE query_id = 'conj_$ID' AND user = '$U1' ASYNC" 2>&1)
denial "$OUT"
echo "victim: $(running "conj_$ID")"
drop_victim "conj_$ID"

echo "-- 5. a qualified column name is not matched"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE processes.query_id = 'own_$ID' ASYNC" 2>&1)
denial "$OUT"

echo "-- 6. holding the grants keeps the ordinary path, including other users' queries"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.processes TO $U1"
$CLICKHOUSE_CLIENT -q "GRANT KILL QUERY ON *.* TO $U1"
start_victim "$U2" "granted_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'granted_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "granted_$ID")"
drop_victim "granted_$ID"

echo "-- 7. KILL QUERY without the SELECT grant, aimed at another user's id"
start_victim "$U2" "foreign_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U3" -q "KILL QUERY WHERE query_id = 'foreign_$ID' ASYNC" 2>&1)
processes_grant_denial "$OUT"
echo "victim: $(running "foreign_$ID")"
drop_victim "foreign_$ID"

echo "-- 8. a recreated name is a different principal and reaches nothing"
$CLICKHOUSE_CLIENT -q "CREATE USER $A1 IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $A1"
start_victim "$A1" "reused_$ID"
$CLICKHOUSE_CLIENT -q "ALTER USER $A1 RENAME TO ${A1}_renamed"
$CLICKHOUSE_CLIENT -q "CREATE USER $A1 IDENTIFIED WITH no_password"
OUT=$($CLICKHOUSE_CLIENT --user "$A1" -q "KILL QUERY WHERE query_id = 'reused_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "victim: $(running "reused_$ID")"
drop_victim "reused_$ID"

echo "-- 9. KILL QUERY without the SELECT grant, aimed at its holder's own id"
start_victim "$U3" "kqown_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U3" -q "KILL QUERY WHERE query_id = 'kqown_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "kqown_$ID")"
drop_victim "kqown_$ID"

echo "-- 10. a renamed principal still reaches the query it started under the old name"
$CLICKHOUSE_CLIENT -q "CREATE USER $A2 IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $A2"
start_victim "$A2" "renamed_$ID"
$CLICKHOUSE_CLIENT -q "ALTER USER $A2 RENAME TO ${A2}_new"
OUT=$($CLICKHOUSE_CLIENT --user "${A2}_new" -q "KILL QUERY WHERE query_id = 'renamed_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "renamed_$ID")"
drop_victim "renamed_$ID"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $U1, $U2, $U3, $A1, ${A1}_renamed, $A2, ${A2}_new"
