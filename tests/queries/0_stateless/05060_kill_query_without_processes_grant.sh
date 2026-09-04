#!/usr/bin/env bash
# Tags: long
# Tag long: every arm starts a query, waits for it to reach the process list and kills it
# synchronously, which costs minutes at the flaky check's rerun count.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U1="u1_${CLICKHOUSE_DATABASE}"
U2="u2_${CLICKHOUSE_DATABASE}"
P_ALL="pall_${CLICKHOUSE_DATABASE}"
P_U1="pu1_${CLICKHOUSE_DATABASE}"
Q1="q1_${CLICKHOUSE_DATABASE}"
F1="f1_${CLICKHOUSE_DATABASE}"
SUFFIX="_${CLICKHOUSE_DATABASE}"
# The marker keeps the victims out of reach of other tests that kill by query text. A KILL cannot land
# while a sleep chunk is in progress, so the interval bounds how long every KILL below waits, and the
# row count keeps the victim's nominal runtime at 10000 seconds.
LONG="SELECT sleep(0.05) FROM numbers(200000) WHERE ignore('$CLICKHOUSE_DATABASE') = 0 SETTINGS max_block_size = 1, max_rows_to_read = 0"
# Two settings nothing else in the test sets, and one that only the caller's own victim sets. None of
# them is randomized by the test runner, so the two key sets differ in both directions on every run.
WIDE="$LONG, totals_auto_threshold = 0.123, insert_quorum_timeout = 600001"
NARROW="$LONG, distributed_connections_pool_size = 1023"
VICTIMS=""

# The native client is used only for what the test asserts on: the KILL QUERY statements, and the
# victims whose rows they match. Setup, cleanup and observation go over HTTP, which does not pay a
# client process startup per statement. HTTP takes one statement per request, so a block is sent as one
# argument per statement.
# --fail-with-body, because a plain curl exits 0 on a server side exception and prints it as if it were
# the result. The diagnostic is on stderr, which the runner fails on where a caller discards the output.
via_http() {
    local stmt
    for stmt in "$@"; do
        ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary "$stmt" \
            || { echo "via_http failed: $stmt" >&2; return 1; }
    done
}

via_http "DROP ROW POLICY IF EXISTS $P_U1 ON system.processes" \
         "DROP ROW POLICY IF EXISTS $P_ALL ON system.processes" \
         "DROP QUOTA IF EXISTS $Q1" \
         "DROP USER IF EXISTS $U1" \
         "DROP USER IF EXISTS $U2" \
         "CREATE USER $U1 IDENTIFIED WITH no_password" \
         "CREATE USER $U2 IDENTIFIED WITH no_password"

start_victim() { # user, query_id, [query]
    $CLICKHOUSE_CLIENT --user "$1" --query_id "$2" -q "${3:-$LONG}" > /dev/null 2>&1 &
    VICTIMS="$VICTIMS $!"
    # Detach so that the shell does not report the signal when a still running victim is stopped.
    disown %% 2> /dev/null
    wait_for_query_to_start "$2" 60
}

alive() { via_http "SELECT count() FROM system.processes WHERE query_id = '$1' SETTINGS use_query_cache = 0"; }

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

# Rows the caller's quota has been charged for so far. An interval row only exists once something has
# been charged, so an absent one reads as 0 rather than as an empty result.
quota_read_rows() {
    via_http "SELECT toUInt64(ifNull(max(read_rows), 0)) FROM system.quotas_usage WHERE quota_name = '$Q1'"
}

reset_arm() {
    for pid in $VICTIMS; do kill -9 "$pid" 2> /dev/null; done
    VICTIMS=""
    via_http "KILL QUERY WHERE query_id LIKE '%\\$SUFFIX' SYNC" \
             "DROP ROW POLICY IF EXISTS $P_U1 ON system.processes" \
             "DROP ROW POLICY IF EXISTS $P_ALL ON system.processes" \
             "DROP QUOTA IF EXISTS $Q1" \
             "REVOKE ALL ON *.* FROM $U1" \
             "REVOKE ALL ON *.* FROM $U2" > /dev/null
}

# A row policy on a system table is server wide, and under
# access_control_improvements/throw_on_unmatched_row_policies a table that has any policy is denied
# to every user no policy applies to. The permissive policy keeps those users unaffected so the
# restrictive one can single out $U1 while other tests run concurrently.
policy_on_u1() { # filter expression
    via_http "CREATE ROW POLICY $P_ALL ON system.processes USING 1 TO ALL" \
             "CREATE ROW POLICY $P_U1 ON system.processes AS RESTRICTIVE USING $1 TO $U1"
}

# 1: the fix. Without SELECT on system.processes a user could not kill even their own query.
start_victim "$U1" "own1$SUFFIX"
echo -n "1 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own1$SUFFIX' SYNC" 2>&1 | killed "own1$SUFFIX"
gone "own1$SUFFIX"; echo "1 alive=$(alive "own1$SUFFIX")"
reset_arm

# 2: the same kill by a user who holds the grant keeps taking the unchanged code path.
via_http "GRANT SELECT ON system.processes TO $U1"
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

# 5: holding KILL QUERY does not widen what a caller who cannot read system.processes reaches. The
# foreign query is ignored, exactly as in arm 3, rather than killed or reported as an error.
via_http "GRANT KILL QUERY ON *.* TO $U1"
start_victim "$U2" "foreign5$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'foreign5$SUFFIX' SYNC" 2>&1)
echo "5 killed=$(printf '%s\n' "$out" | killed "foreign5$SUFFIX")"
echo "5 exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "5 alive=$(alive "foreign5$SUFFIX")"
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

# 7: the caller's row policy on system.processes decides which of their queries they can see, and the
# policies that decided it are named in the caller's own system.query_log row.
policy_on_u1 "query_id != 'own7$SUFFIX'"
start_victim "$U1" "own7$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" --query_id "kill7$SUFFIX" -q "KILL QUERY WHERE query_id = 'own7$SUFFIX' SYNC" 2>&1)
echo "7 hidden killed=$(printf '%s\n' "$out" | killed "own7$SUFFIX")"
echo "7 hidden exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "7 hidden alive=$(alive "own7$SUFFIX")"
via_http "SYSTEM FLUSH LOGS query_log"
echo "7 hidden used_policy=$(via_http "
    SELECT toUInt8(ifNull(max(has(used_row_policies, '$P_U1 ON system.processes')), 0))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = 'kill7$SUFFIX'")"
via_http "DROP ROW POLICY $P_U1 ON system.processes"
echo -n "7 admitted killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own7$SUFFIX' SYNC" 2>&1 | killed "own7$SUFFIX"
gone "own7$SUFFIX"; echo "7 admitted alive=$(alive "own7$SUFFIX")"
reset_arm

# 8: additional_table_filters keeps applying on the path taken by a caller who holds the grant.
via_http "GRANT SELECT ON system.processes TO $U1"
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

# 10: a malformed predicate is diagnosed even when the caller's row policy admitted no row at all. The
# policy rejects every row the caller owns, so the block the predicate runs over is empty while still
# carrying its columns. Whether a statement is refused must not depend on a policy the caller usually
# cannot see, and the privileged path refuses it whatever the row count.
policy_on_u1 "0"
start_victim "$U1" "own10$SUFFIX"
echo -n "10 bad_predicate exception="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE no_such_column = 1 SYNC" 2>&1 | matched "DB::Exception"
# The same policy with a predicate that resolves, so the arm above cannot pass because anything at all
# fails under a policy that rejects everything. The path is otherwise silent, and the victim the policy
# hid from the caller is still running.
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own10$SUFFIX' SYNC" 2>&1)
echo "10 control exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "10 control killed=$(printf '%s\n' "$out" | killed "own10$SUFFIX")"
echo "10 control alive=$(alive "own10$SUFFIX")"
reset_arm

# 12: a grant covering only some of the columns the statement reads still cannot read the table.
via_http "GRANT SELECT(query_id, user) ON system.processes TO $U1"
start_victim "$U1" "own12$SUFFIX"
echo -n "12 killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own12$SUFFIX' SYNC" 2>&1 | killed "own12$SUFFIX"
gone "own12$SUFFIX"; echo "12 alive=$(alive "own12$SUFFIX")"
reset_arm

# 13: a policy qualified with the table name resolves, and is applied. The policy rejects the caller's
# own victim, so a policy that compiled but was never applied would kill a query that must survive.
policy_on_u1 "processes.query_id != 'own13$SUFFIX'"
start_victim "$U1" "own13$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own13$SUFFIX' SYNC" 2>&1)
echo "13 hidden killed=$(printf '%s\n' "$out" | killed "own13$SUFFIX")"
echo "13 hidden exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "13 hidden alive=$(alive "own13$SUFFIX")"
via_http "DROP ROW POLICY $P_U1 ON system.processes"
echo -n "13 admitted killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own13$SUFFIX' SYNC" 2>&1 | killed "own13$SUFFIX"
gone "own13$SUFFIX"; echo "13 admitted alive=$(alive "own13$SUFFIX")"
reset_arm

# 14: additional_table_filters must not reach the read that runs with full access.
start_victim "$U1" "own14$SUFFIX"
echo -n "14 killed="
$CLICKHOUSE_CLIENT --user "$U1" --additional_table_filters="{'system.processes': '1=0'}" \
    -q "KILL QUERY WHERE query_id = 'own14$SUFFIX' SYNC" 2>&1 | killed "own14$SUFFIX"
gone "own14$SUFFIX"; echo "14 alive=$(alive "own14$SUFFIX")"
reset_arm

# 15: a policy may contain a subquery, is applied, and resolves when spelled with the database too. An
# unbuilt set would surface as an exception, which is a different observable from a skipped policy.
policy_on_u1 "system.processes.query_id NOT IN (SELECT 'own15$SUFFIX')"
start_victim "$U1" "own15$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own15$SUFFIX' SYNC" 2>&1)
echo "15 hidden killed=$(printf '%s\n' "$out" | killed "own15$SUFFIX")"
echo "15 hidden exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "15 hidden alive=$(alive "own15$SUFFIX")"
via_http "DROP ROW POLICY $P_U1 ON system.processes"
echo -n "15 admitted killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own15$SUFFIX' SYNC" 2>&1 | killed "own15$SUFFIX"
gone "own15$SUFFIX"; echo "15 admitted alive=$(alive "own15$SUFFIX")"
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
via_http "GRANT SELECT(query_id, user, query) ON system.processes TO $U1"
start_victim "$U2" "foreign17$SUFFIX"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'foreign17$SUFFIX' SYNC" 2>&1)
echo "17 killed=$(printf '%s\n' "$out" | killed "foreign17$SUFFIX")"
echo "17 exception=$(printf '%s\n' "$out" | matched "attempts to kill query created by")"
echo "17 alive=$(alive "foreign17$SUFFIX")"
reset_arm

# 18: the caller's quota is charged for the rows their own statement read, and not for the read behind
# it, which scans every user's row. A quota counts per user rather than per query, so the charge is
# measured around a statement the caller issues while owning nothing else: their only row is then that
# statement itself, so the charge is exactly one, while the six foreign victims still running would make
# a leaked scan cost at least eight. That the caller's own read is metered at all is asserted in arm 19,
# whose counter is per query.
via_http "CREATE QUOTA $Q1 FOR INTERVAL 1 HOUR MAX READ ROWS = 1000000 TO $U1"
for n in 1 2 3 4 5 6; do start_victim "$U2" "foreign18$n$SUFFIX"; done
start_victim "$U1" "own18$SUFFIX"
echo "18 own=$(alive "own18$SUFFIX") foreign=$(via_http "SELECT count() FROM system.processes WHERE query_id LIKE 'foreign18%\\$SUFFIX'")"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id LIKE '%\\$SUFFIX' SYNC" 2>&1)
echo "18 killed=$(printf '%s\n' "$out" | killed "own18$SUFFIX")"
echo "18 exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
gone "own18$SUFFIX"
before=$(quota_read_rows)
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'nomatch$SUFFIX' SYNC" > /dev/null
after=$(quota_read_rows)
echo "18 charge=$(( after - before ))"
reset_arm

# 19: the read behind the statement scans every user's row, so what it read is neither reported to the
# caller nor charged to their profile events. Both channels are bounded by what the caller's own rows
# cost: the progress their statement reports back, and the SelectedRows its own query_log row carries.
# Six foreign victims run so that either channel would leave the band if it reported the wider read.
# Over HTTP the response is finalized before the log row is written, so that row is polled for.
for n in 1 2 3 4 5 6; do start_victim "$U2" "foreign19$n$SUFFIX"; done
start_victim "$U1" "own19$SUFFIX"
progress_rows=$(${CLICKHOUSE_CURL} -sS -D - -o /dev/null \
    "${CLICKHOUSE_URL}&user=$U1&query_id=kill19$SUFFIX&http_wait_end_of_query=1" \
    --data-binary "KILL QUERY WHERE query_id = 'own19$SUFFIX' SYNC" \
    | grep -a -i -m1 '^X-ClickHouse-Summary' | grep -o -m1 '"read_rows":"[0-9]*"' | tr -dc 0-9)
echo "19 progress_in_band=$(( ${progress_rows:-0} >= 1 && ${progress_rows:-0} <= 6 ? 1 : 0 ))"
gone "own19$SUFFIX"; echo "19 alive=$(alive "own19$SUFFIX")"
via_http "SYSTEM FLUSH LOGS query_log"
log_start=$EPOCHSECONDS
while [[ $(via_http "
    SELECT count() FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = 'kill19$SUFFIX'") == 0 ]]; do
    if ((EPOCHSECONDS - log_start > 60)); then
        echo "Timeout waiting for the query_log row of kill19$SUFFIX" >&2
        break
    fi
    sleep 0.5
    via_http "SYSTEM FLUSH LOGS query_log"
done
echo "19 selected_rows_in_band=$(via_http "
    SELECT toUInt8(ifNull(max(ProfileEvents['SelectedRows']), 0) BETWEEN 1 AND 6)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = 'kill19$SUFFIX'")"
reset_arm

# 20: an index into a LowCardinality dictionary nested in a Map must not depend on a row the caller
# cannot see. The foreign victim starts first and the two victims set settings the other does not, so a
# dictionary built over the wider block gives the caller's own row an index above its own key count,
# while one built over the caller's own rows alone cannot.
start_victim "$U2" "foreign20$SUFFIX" "$WIDE"
start_victim "$U1" "own20$SUFFIX" "$NARROW"
out=$($CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own20$SUFFIX'
    AND lowCardinalityIndices(arrayJoin(Settings.keys)) > length(Settings.keys) SYNC" 2>&1)
echo "20 shifted killed=$(printf '%s\n' "$out" | killed "own20$SUFFIX")"
echo "20 shifted exception=$(printf '%s\n' "$out" | matched "DB::Exception")"
echo "20 shifted alive=$(alive "own20$SUFFIX")"
# The same reader with a bound every index satisfies, so the arm above cannot pass by not evaluating.
echo -n "20 control killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE query_id = 'own20$SUFFIX'
    AND lowCardinalityIndices(arrayJoin(Settings.keys)) >= 1 SYNC" 2>&1 | killed "own20$SUFFIX"
gone "own20$SUFFIX"; echo "20 control alive=$(alive "own20$SUFFIX")"
reset_arm

# 21: a predicate qualified with the database resolves, so which spellings the statement accepts does
# not depend on the caller's grant.
start_victim "$U1" "own21a$SUFFIX"
echo -n "21 grant_free killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE system.processes.query_id = 'own21a$SUFFIX' SYNC" 2>&1 | killed "own21a$SUFFIX"
gone "own21a$SUFFIX"; echo "21 grant_free alive=$(alive "own21a$SUFFIX")"
via_http "GRANT SELECT ON system.processes TO $U1"
start_victim "$U1" "own21b$SUFFIX"
echo -n "21 granted killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE system.processes.query_id = 'own21b$SUFFIX' SYNC" 2>&1 | killed "own21b$SUFFIX"
gone "own21b$SUFFIX"; echo "21 granted alive=$(alive "own21b$SUFFIX")"
reset_arm

# 22: a matcher carries its qualifier in a node of its own, so it needs the same treatment as a column.
start_victim "$U1" "own22$SUFFIX"
echo -n "22 asterisk killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE notEmpty(toString(tuple(system.processes.*)))
    AND query_id = 'own22$SUFFIX' SYNC" 2>&1 | killed "own22$SUFFIX"
gone "own22$SUFFIX"; echo "22 asterisk alive=$(alive "own22$SUFFIX")"
reset_arm

# 23: a function body is substituted later than the statement's own AST is read, so a qualifier inside
# one is shortened only if the body is inlined first. Both paths accept the same spelling.
# The analyzer is pinned because the old one resolves no qualifier inside a function body on either
# path, so under it the two halves would compare a spelling neither path has ever accepted.
via_http "CREATE OR REPLACE FUNCTION $F1 AS (id) -> system.processes.query_id = id"
start_victim "$U1" "own23a$SUFFIX"
echo -n "23 grant_free killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE $F1('own23a$SUFFIX') SYNC
    SETTINGS enable_analyzer = 1" 2>&1 | killed "own23a$SUFFIX"
gone "own23a$SUFFIX"; echo "23 grant_free alive=$(alive "own23a$SUFFIX")"
via_http "GRANT SELECT ON system.processes TO $U1"
start_victim "$U1" "own23b$SUFFIX"
echo -n "23 granted killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE $F1('own23b$SUFFIX') SYNC
    SETTINGS enable_analyzer = 1" 2>&1 | killed "own23b$SUFFIX"
gone "own23b$SUFFIX"; echo "23 granted alive=$(alive "own23b$SUFFIX")"
via_http "DROP FUNCTION $F1"
reset_arm

# 24: a matcher argument is a form that substitution refuses, and the analyzer accepts it, so the
# predicate has to reach the read unsubstituted rather than carrying the refusal.
via_http "CREATE OR REPLACE FUNCTION $F1 AS (x) -> x = 'own24$SUFFIX'"
start_victim "$U1" "own24$SUFFIX"
echo -n "24 matcher_arg killed="
$CLICKHOUSE_CLIENT --user "$U1" -q "KILL QUERY WHERE $F1(COLUMNS('^query_id\$')) SYNC
    SETTINGS enable_analyzer = 1" 2>&1 | killed "own24$SUFFIX"
gone "own24$SUFFIX"; echo "24 matcher_arg alive=$(alive "own24$SUFFIX")"
via_http "DROP FUNCTION $F1"
reset_arm

via_http "DROP USER $U1, $U2"
