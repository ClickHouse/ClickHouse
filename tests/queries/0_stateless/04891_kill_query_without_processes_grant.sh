#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U1="u1_${CLICKHOUSE_DATABASE}"
U2="u2_${CLICKHOUSE_DATABASE}"
P_ALL="pall_${CLICKHOUSE_DATABASE}"
P_U1="pu1_${CLICKHOUSE_DATABASE}"
SUFFIX="_${CLICKHOUSE_DATABASE}"
# The marker keeps the victims out of reach of other tests that kill by query text.
LONG="SELECT sleep(1) FROM numbers(10000) WHERE ignore('$CLICKHOUSE_DATABASE') = 0 SETTINGS max_block_size = 1, max_rows_to_read = 0"
VICTIMS=""

$CLICKHOUSE_CLIENT -q "
    DROP ROW POLICY IF EXISTS $P_U1 ON system.processes;
    DROP ROW POLICY IF EXISTS $P_ALL ON system.processes;
    DROP USER IF EXISTS $U1;
    DROP USER IF EXISTS $U2;
    CREATE USER $U1 IDENTIFIED WITH no_password;
    CREATE USER $U2 IDENTIFIED WITH no_password;
"

start_victim() { # user, query_id
    $CLICKHOUSE_CLIENT --user "$1" --query_id "$2" -q "$LONG" > /dev/null 2>&1 &
    VICTIMS="$VICTIMS $!"
    # Detach so that the shell does not report the signal when a still running victim is stopped.
    disown %% 2> /dev/null
    local start=$EPOCHSECONDS
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$2'") == 0 ]]; do
        if ((EPOCHSECONDS - start > 60)); then
            echo "Timeout waiting for query $2 to start" >&2
            exit 1
        fi
        sleep 0.1
    done
}

alive() { $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$1'"; }

# A killed query leaves the process list asynchronously, so poll rather than sampling once.
gone() {
    local start=$EPOCHSECONDS
    while [[ $(alive "$1") != 0 ]]; do
        if ((EPOCHSECONDS - start > 60)); then
            return 1
        fi
        sleep 0.1
    done
    return 0
}

# Whether KILL QUERY named the query in a result row. The query id is matched between the tab
# separators of the result row, because the client also echoes the failing statement, which contains
# the same id.
killed() { grep -c -F "$(printf '\t%s\t' "$1")" | sed 's/^[1-9][0-9]*$/1/'; }

matched() { grep -c -F "$1" | sed 's/^[1-9][0-9]*$/1/'; }

reset_arm() {
    for pid in $VICTIMS; do kill -9 "$pid" 2> /dev/null; done
    VICTIMS=""
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '%\\$SUFFIX' SYNC" > /dev/null
    $CLICKHOUSE_CLIENT -q "
        DROP ROW POLICY IF EXISTS $P_U1 ON system.processes;
        DROP ROW POLICY IF EXISTS $P_ALL ON system.processes;
        REVOKE ALL ON *.* FROM $U1;
        REVOKE ALL ON *.* FROM $U2;
    "
}

# A row policy on a system table is server wide, and under
# access_control_improvements/throw_on_unmatched_row_policies a table that has any policy is denied
# to every user no policy applies to. The permissive policy keeps those users unaffected so the
# restrictive one can single out $U1 while other tests run concurrently.
policy_on_u1() { # filter expression
    $CLICKHOUSE_CLIENT -q "
        CREATE ROW POLICY $P_ALL ON system.processes USING 1 TO ALL;
        CREATE ROW POLICY $P_U1 ON system.processes AS RESTRICTIVE USING $1 TO $U1;
    "
}

# 1: the fix. Without SELECT on system.processes a user could not kill even their own query.
start_victim "$U1" "own1$SUFFIX"
echo -n "1 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own1$SUFFIX' SYNC" 2>&1 | killed "own1$SUFFIX"
gone "own1$SUFFIX"; echo "1 alive=$(alive "own1$SUFFIX")"
reset_arm

# 2: the same kill by a user who holds the grant keeps taking the unchanged code path.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.processes TO $U1"
start_victim "$U1" "own2$SUFFIX"
echo -n "2 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own2$SUFFIX' SYNC" 2>&1 | killed "own2$SUFFIX"
gone "own2$SUFFIX"; echo "2 alive=$(alive "own2$SUFFIX")"
reset_arm

# 3: a query of another user is ignored rather than reported as an error. "exception" is what
# distinguishes being ignored from being refused; "alive" alone holds either way.
start_victim "$U2" "foreign3$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'foreign3$SUFFIX' SYNC" 2>&1)
echo "3 killed=$(printf '%s\n' "$out" | killed "foreign3$SUFFIX")"
echo "3 exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "3 alive=$(alive "foreign3$SUFFIX")"
reset_arm

# 4: a predicate qualified with the table name resolves.
start_victim "$U1" "own4$SUFFIX"
echo -n "4 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE processes.query_id = 'own4$SUFFIX' SYNC" 2>&1 | killed "own4$SUFFIX"
gone "own4$SUFFIX"; echo "4 alive=$(alive "own4$SUFFIX")"
reset_arm

# 5: KILL QUERY alone is enough to kill another user's query.
$CLICKHOUSE_CLIENT -q "GRANT KILL QUERY ON *.* TO $U1"
start_victim "$U2" "foreign5$SUFFIX"
echo -n "5 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'foreign5$SUFFIX' SYNC" 2>&1 | killed "foreign5$SUFFIX"
gone "foreign5$SUFFIX"; echo "5 alive=$(alive "foreign5$SUFFIX")"
reset_arm

# 6: a predicate matching everything reaches the caller's own queries only.
start_victim "$U1" "own6a$SUFFIX"
start_victim "$U1" "own6b$SUFFIX"
start_victim "$U2" "foreign6$SUFFIX"
echo -n "6 distinct_users_killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id LIKE '%\\$SUFFIX' SYNC" | cut -f 3 | sort -u | matched "$U1"
gone "own6a$SUFFIX"
gone "own6b$SUFFIX"
echo "6 own=$(( $(alive "own6a$SUFFIX") + $(alive "own6b$SUFFIX") )) foreign=$(alive "foreign6$SUFFIX")"
reset_arm

# 7: the caller's row policy on system.processes decides which of their queries they can see.
policy_on_u1 "query_id != 'own7$SUFFIX'"
start_victim "$U1" "own7$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own7$SUFFIX' SYNC" 2>&1)
echo "7 hidden killed=$(printf '%s\n' "$out" | killed "own7$SUFFIX")"
echo "7 hidden exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "7 hidden alive=$(alive "own7$SUFFIX")"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY $P_U1 ON system.processes"
echo -n "7 admitted killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own7$SUFFIX' SYNC" 2>&1 | killed "own7$SUFFIX"
gone "own7$SUFFIX"; echo "7 admitted alive=$(alive "own7$SUFFIX")"
reset_arm

# 8: additional_table_filters keeps applying on the path taken by a caller who holds the grant.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.processes TO $U1"
start_victim "$U1" "own8$SUFFIX"
echo -n "8 killed="
$CLICKHOUSE_CLIENT --user "$U1" --additional_table_filters="{'system.processes': '1=0'}" \
    -q "KILL QUERY WHERE query_id = 'own8$SUFFIX' SYNC" 2>&1 | killed "own8$SUFFIX"
echo "8 alive=$(alive "own8$SUFFIX")"
reset_arm

# 9: the predicate is never evaluated over a row the caller may not kill.
start_victim "$U2" "foreign9$SUFFIX"
# The oracle is "no exception at all": the client echoes the failing statement, so grepping for the
# message of the throwIf would also match a statement that never ran.
echo -n "9 exception="
$CLICKHOUSE_CLIENT --user "$U1" -q \
    "KILL QUERY WHERE throwIf(user != currentUser(), 'the predicate saw a foreign row') SYNC" 2>&1 \
    | matched "DB::Exception"
echo "9 alive=$(alive "foreign9$SUFFIX")"
reset_arm

# 12: a grant covering only some of the columns the statement reads still cannot read the table.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(query_id, user) ON system.processes TO $U1"
start_victim "$U1" "own12$SUFFIX"
echo -n "12 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own12$SUFFIX' SYNC" 2>&1 | killed "own12$SUFFIX"
gone "own12$SUFFIX"; echo "12 alive=$(alive "own12$SUFFIX")"
reset_arm

# 13: a policy qualified with the table name resolves.
policy_on_u1 "processes.user = currentUser()"
start_victim "$U1" "own13$SUFFIX"
start_victim "$U2" "foreign13$SUFFIX"
echo -n "13 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id LIKE '%\\$SUFFIX' SYNC" 2>&1 | killed "own13$SUFFIX"
gone "own13$SUFFIX"
echo "13 own=$(alive "own13$SUFFIX") foreign=$(alive "foreign13$SUFFIX")"
reset_arm

# 14: additional_table_filters must not reach the read that runs with full access.
start_victim "$U1" "own14$SUFFIX"
echo -n "14 killed="
$CLICKHOUSE_CLIENT --user "$U1" --additional_table_filters="{'system.processes': '1=0'}" \
    -q "KILL QUERY WHERE query_id = 'own14$SUFFIX' SYNC" 2>&1 | killed "own14$SUFFIX"
gone "own14$SUFFIX"; echo "14 alive=$(alive "own14$SUFFIX")"
reset_arm

# 15: a policy may contain a subquery.
policy_on_u1 "user IN (SELECT currentUser())"
start_victim "$U1" "own15$SUFFIX"
echo -n "15 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own15$SUFFIX' SYNC" 2>&1 | killed "own15$SUFFIX"
gone "own15$SUFFIX"; echo "15 alive=$(alive "own15$SUFFIX")"
reset_arm

# 16: the caller's width and AST limits bound their own predicate, not the read behind it.
start_victim "$U1" "own16$SUFFIX"
echo -n "16 killed="
$CLICKHOUSE_CLIENT --user "$U1" --max_columns_to_read=3 --max_expanded_ast_elements=100 \
    -q "KILL QUERY WHERE query_id = 'own16$SUFFIX' SYNC" 2>&1 | killed "own16$SUFFIX"
gone "own16$SUFFIX"; echo "16 alive=$(alive "own16$SUFFIX")"
reset_arm

# 17: a grant covering exactly the columns the statement reads is enough to keep the caller on the
# unchanged path, where a match that names only another user's query is still refused. This is what
# distinguishes the per-column grant check from a table-wide one, which would divert this caller.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(query_id, user, query) ON system.processes TO $U1"
start_victim "$U2" "foreign17$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'foreign17$SUFFIX' SYNC" 2>&1)
echo "17 killed=$(printf '%s\n' "$out" | killed "foreign17$SUFFIX")"
echo "17 exception=$(printf '%s\n' "$out" | matched "attempts to kill query created by")"
echo "17 alive=$(alive "foreign17$SUFFIX")"
reset_arm

$CLICKHOUSE_CLIENT -q "DROP USER $U1, $U2"
