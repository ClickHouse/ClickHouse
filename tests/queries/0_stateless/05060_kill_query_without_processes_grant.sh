#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U1="u1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U2="u2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U3="u3_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U4="u4_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U5="u5_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U6="u6_${CLICKHOUSE_TEST_UNIQUE_NAME}"
A1="a1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
A2="a2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $U1, $U2, $U3, $U4, $U5, $U6, $A1, ${A1}_renamed, $A2, ${A2}_new"
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

# A no-op on the reduced path is reported as an exception while `kill_throw_if_noop` is on.
function noop_throw()
{
    if grep -q -F "NOTHING_TO_KILL" <<< "$1"; then
        echo "throws NOTHING_TO_KILL"
    else
        echo "unexpected: $1"
    fi
}

# A `SELECT` holder aimed at somebody else's id must be refused by the ordinary path, which reports
# this instead of naming a grant. Answering it with an empty result would mean the reduced path had
# taken a caller who can read the table, and with it their row policies.
function foreign_kill_denial()
{
    if grep -q -F "attempts to kill query created by" <<< "$1"; then
        echo "refused: attempts to kill query created by another user"
    else
        echo "unexpected: $1"
    fi
}

# $1 = query_id, $2 = marker in the query text. Bounded: the id is held by one query, that one.
function wait_sole_holder()
{
    for _ in {1..150}; do
        if [[ "$($CLICKHOUSE_CLIENT -q \
            "SELECT countIf(query LIKE '%$2%') = 1 AND countIf(query NOT LIKE '%$2%') = 0 \
             FROM system.processes WHERE query_id = '$1'")" == "1" ]]; then
            echo "sole holder"
            return
        fi
        sleep 0.2
    done
    echo "not the sole holder"
}

function names_marker()
{
    if grep -q -F "$2" <<< "$1"; then
        echo "the query still running"
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

echo "-- 2. another user's id: no rows, no error, victim untouched, kill_throw_if_noop off"
start_victim "$U2" "other_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'other_$ID' ASYNC SETTINGS kill_throw_if_noop = 0" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "victim: $(running "other_$ID")"
drop_victim "other_$ID"

echo "-- 3. an id nobody is running: no rows, no error, kill_throw_if_noop off"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'absent_$ID' ASYNC SETTINGS kill_throw_if_noop = 0" 2>&1)
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

echo "-- 8. a recreated name is a different principal and reaches nothing, kill_throw_if_noop off"
$CLICKHOUSE_CLIENT -q "CREATE USER $A1 IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $A1"
start_victim "$A1" "reused_$ID"
$CLICKHOUSE_CLIENT -q "ALTER USER $A1 RENAME TO ${A1}_renamed"
$CLICKHOUSE_CLIENT -q "CREATE USER $A1 IDENTIFIED WITH no_password"
OUT=$($CLICKHOUSE_CLIENT --user "$A1" -q "KILL QUERY WHERE query_id = 'reused_$ID' ASYNC SETTINGS kill_throw_if_noop = 0" 2>&1)
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

echo "-- 11. the literal on the left of the equality is the same shape"
start_victim "$U2" "rev_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" -q "KILL QUERY WHERE 'rev_$ID' = query_id ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "rev_$ID")"
drop_victim "rev_$ID"

echo "-- 12. SYNC reports the query stopped, through the same ownership check"
start_victim "$U2" "sync_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" -q "KILL QUERY WHERE query_id = 'sync_$ID' SYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "sync_$ID")"
drop_victim "sync_$ID"

echo "-- 13. a SELECT holder keeps the ordinary path, so its refusal is the ordinary one"
$CLICKHOUSE_CLIENT -q "CREATE USER $U4 IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $U4"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.processes TO $U4"
start_victim "$U2" "sel_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U4" -q "KILL QUERY WHERE query_id = 'sel_$ID' ASYNC" 2>&1)
foreign_kill_denial "$OUT"
echo "victim: $(running "sel_$ID")"
drop_victim "sel_$ID"

echo "-- 14. a statement naming its own id skips itself instead of cancelling itself, kill_throw_if_noop off"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" --query_id "self_$ID" -q \
    "KILL QUERY WHERE query_id = 'self_$ID' ASYNC SETTINGS kill_throw_if_noop = 0" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"

echo "-- 15. TEST names the match without cancelling it"
start_victim "$U2" "test_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" -q "KILL QUERY WHERE query_id = 'test_$ID' TEST" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(running "test_$ID")"
drop_victim "test_$ID"

echo "-- 16. a partial column grant is not the grant the ordinary path needs"
# Its holder cannot complete that read either, so narrowing a grant must not take the ability away.
$CLICKHOUSE_CLIENT -q "CREATE USER $U5 IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $U5"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(query_id, user) ON system.processes TO $U5"
OUT=$($CLICKHOUSE_CLIENT --user "$U5" -q "SELECT query_id, user, query FROM system.processes FORMAT Null" 2>&1)
processes_grant_denial "$OUT"
start_victim "$U5" "part_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U5" -q "KILL QUERY WHERE query_id = 'part_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "part_$ID")"
drop_victim "part_$ID"

echo "-- 17. a grant of exactly the columns the read names keeps the ordinary path"
$CLICKHOUSE_CLIENT -q "CREATE USER $U6 IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $U6"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(query_id, user, query) ON system.processes TO $U6"
start_victim "$U2" "cols_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U6" -q "KILL QUERY WHERE query_id = 'cols_$ID' ASYNC" 2>&1)
foreign_kill_denial "$OUT"
echo "victim: $(running "cols_$ID")"
drop_victim "cols_$ID"

echo "-- 18. an id taken over by replace_running_query reaches the query now running under it"
$CLICKHOUSE_CLIENT --user "$U2" --query_id "repl_$ID" -q \
    "SELECT 'repl_a_$ID', sleep(0.1) FROM numbers(100000) SETTINGS max_block_size = 1, max_rows_to_read = 0" \
    > /dev/null 2>&1 &
echo "first: $(wait_sole_holder "repl_$ID" "repl_a_$ID")"
$CLICKHOUSE_CLIENT --user "$U2" --query_id "repl_$ID" -q \
    "SELECT 'repl_b_$ID', sleep(0.1) FROM numbers(100000) SETTINGS max_block_size = 1, max_rows_to_read = 0, \
     replace_running_query = 1, replace_running_query_max_wait_ms = 30000" \
    > /dev/null 2>&1 &
echo "replacement: $(wait_sole_holder "repl_$ID" "repl_b_$ID")"
# `$U2` holds neither grant, so a non-empty result can only have come from the reduced path.
OUT=$($CLICKHOUSE_CLIENT --user "$U2" -q "KILL QUERY WHERE query_id = 'repl_$ID' SYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "names: $(names_marker "$OUT" "repl_b_$ID")"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "repl_$ID")"
drop_victim "repl_$ID"

echo "-- 19. kill_throw_if_noop on (the default) reports the reduced path no-op as an exception"
# `$U1` holds both grants since section 6, so `$U2` names an id run by `$U5` here.
start_victim "$U5" "noop_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" -q "KILL QUERY WHERE query_id = 'noop_$ID' ASYNC" 2>&1)
noop_throw "$OUT"
echo "victim: $(running "noop_$ID")"
drop_victim "noop_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" -q "KILL QUERY WHERE query_id = 'absent2_$ID' ASYNC" 2>&1)
noop_throw "$OUT"
OUT=$($CLICKHOUSE_CLIENT --user "$U2" --query_id "self2_$ID" -q \
    "KILL QUERY WHERE query_id = 'self2_$ID' ASYNC" 2>&1)
noop_throw "$OUT"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $U1, $U2, $U3, $U4, $U5, $U6, $A1, ${A1}_renamed, $A2, ${A2}_new"
