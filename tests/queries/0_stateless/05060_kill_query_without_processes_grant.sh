#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U1="u1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U2="u2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $U1, $U2"
$CLICKHOUSE_CLIENT -q "CREATE USER $U1, $U2 IDENTIFIED WITH no_password"
# Deliberately no SELECT on system.processes and no KILL QUERY for either user.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $U1, $U2"

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

echo "-- 4. a condition that is not a by-id self kill still needs the grant"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE user = currentUser() ASYNC" 2>&1)
denial "$OUT"

echo "-- 5. the canonical predicate, which ON CLUSTER queues verbatim"
start_victim "$U1" "canon_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE query_id = 'canon_$ID' AND user = '$U1' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "canon_$ID")"
drop_victim "canon_$ID"

echo "-- 6. a foreign user literal does not qualify"
start_victim "$U2" "foreign_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE query_id = 'foreign_$ID' AND user = '$U2' ASYNC" 2>&1)
denial "$OUT"
echo "victim: $(running "foreign_$ID")"
drop_victim "foreign_$ID"

echo "-- 7. a qualified column name is not matched"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE processes.query_id = 'own_$ID' ASYNC" 2>&1)
denial "$OUT"

echo "-- 8. the same predicate written as the function calls it serializes to"
start_victim "$U1" "serialized_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE and(equals(query_id, 'serialized_$ID'), equals(user, '$U1')) ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "serialized_$ID")"
drop_victim "serialized_$ID"

echo "-- 9. holding the grants keeps the ordinary path, including other users' queries"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.processes TO $U1"
$CLICKHOUSE_CLIENT -q "GRANT KILL QUERY ON *.* TO $U1"
start_victim "$U2" "granted_$ID"
OUT=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'granted_$ID' ASYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"
echo "status: $(echo "$OUT" | cut -f1)"
echo "victim: $(wait_gone "granted_$ID")"
drop_victim "granted_$ID"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $U1, $U2"
