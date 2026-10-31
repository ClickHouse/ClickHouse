#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and are not
#              allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: the proof events below are server-global, and the assertions require them to stay
#              put, so no other test may run oracle checks against the same server meanwhile.
#
# `arrayJoin` (and its case-insensitive alias `unnest`) is the one function that changes the number
# of rows. The oracles compare a query against a rewrite of it that has to return the same rows, so
# a query calling it must not be checked at all. An `APPLY` column transformer is a spelling of the
# call that was missed: `SELECT * APPLY (x -> arrayJoin([x, x + 1])) FROM t WHERE i > 1` multiplies
# the rows of every expanded column, the query was checked anyway, and the client was handed a false
# `AST_FUZZER_ORACLE_MISMATCH` - a count of 8 against a count of 2 - for a query that answers
# correctly. The transformer keeps its lambda, its parameters and a bare function name outside the
# node's children, which is why walking the children alone does not find the call.
#
# The shape has to come from the user's own query: the fuzzer builds an `APPLY` only out of a fixed
# three-function list and never rewrites a lambda it finds, so it cannot invent this one.
#
# The queries below are still CHECKED, on purpose, so their probes read the mismatch counter and not
# the check counter: one oracle rewrites only the `WHERE` predicate and leaves the select list alone,
# so both sides multiply the rows identically and it stays safe to run on a row-multiplying query.
# Do not "repair" such a probe into a "not checked" assertion - it would be a false claim. Dropping
# the `WHERE`, so that that oracle bails out, is not a way around it either: every assertion here is
# a property of the query the server FUZZED, and the fuzzer adds a `WHERE` to roughly one mutant in
# sixty, after which that oracle legitimately runs and the check counter legitimately moves.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL UDFs are global rather than database-scoped, so the name embeds $CLICKHOUSE_DATABASE to avoid
# collisions with concurrent test runs.
UDF="${CLICKHOUSE_DATABASE}_oracle_apply_udf"
ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_apply_array_join;
    CREATE TABLE oracle_apply_array_join (i Int32, b Int32) ENGINE = MergeTree ORDER BY i;
    INSERT INTO oracle_apply_array_join VALUES (1, 10), (2, 20), (3, 30);
    CREATE OR REPLACE FUNCTION $UDF AS (x) -> arrayJoin([x, x + 1]);
"

get_counter()
{
    $CLICKHOUSE_CLIENT --query "SELECT toInt64(sum(value)) FROM system.events WHERE event = '${1}'"
}

# All rounds of one attempt travel in a single invocation with the counter reading last, so batching
# only removes client start-up cost - which is what dominates the run time of a test like this one.
# Each round is still fuzzed and checked on its own, so the rounds are what carries the detection
# power: whether a round that should be refused produces a mismatch depends on the mutation it drew,
# so six of them make "this spelling stopped being refused" show up essentially every time, while
# costing no extra invocation.
#
# `send_logs_level = 'fatal'` suppresses the expected error-level log lines from mutations that
# produce valid-but-nonsense queries. No `FORMAT Null` on a fuzzed query: the oracles skip a query
# carrying an explicit FORMAT clause, which would make every assertion below vacuous. `--ignore-error`
# keeps the rounds independent of one another and lets the trailing counter reading still happen when
# a round raised. `SETTINGS ast_fuzzer_runs = 0` keeps the fuzzer off that reading.
#
# stderr goes to a file rather than to the test's: it carries the client-visible error text the
# assertions read back, and clickhouse-test fails a test whose stderr is non-empty regardless of
# stdout, so letting the expected log noise through would fail the test unconditionally.
run_fuzzed_rounds()
{
    local query="$1"
    local event="$2"

    $CLICKHOUSE_CLIENT --ignore-error --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        $query;
        $query;
        $query;
        $query;
        $query;
        $query;
        SELECT toInt64(sum(value)) FROM system.events
        WHERE event = '$event' SETTINGS ast_fuzzer_runs = 0;
    " 2>"$ERR" | tail -n 1
}

# A counter delta is a property of the query the server FUZZED, not of the one written here, and one
# mutation in ~1500 replaces the select-list element outright - so it can drop the element carrying
# the `arrayJoin`, leaving a plain query the oracles rightly check. That moves the counter while the
# refusal is perfectly intact. Hence: retry while the delta is non-zero, keep the smallest, and read
# back the stderr of the attempt that produced it. The outcomes are asymmetric, which is what makes
# this sound - a refusal that stopped seeing the call moves the counter on EVERY attempt, whereas the
# mutation is independent per attempt - and it costs nothing when the first attempt is already zero.
#
# The validity reading in the same invocation as the baseline is what separates "the refusal worked"
# from "this query is simply broken": a query that fails before reaching the oracles produces a zero
# delta too. The fuzzer is off in that run, so it cannot move the counter, and `FORMAT Null` is safe
# there for the same reason.
smallest_delta()
{
    local query="$1"
    local event="$2"
    local best=
    local before
    local after
    local delta

    for _ in 1 2 3
    do
        if ! before=$($CLICKHOUSE_CLIENT --query "
            $query FORMAT Null;
            SELECT toInt64(sum(value)) FROM system.events WHERE event = '$event';
        " 2>/dev/null)
        then
            echo "invalid"
            return
        fi

        after=$(run_fuzzed_rounds "$query" "$event")
        delta=$((after - before))
        if [[ -z "$best" || "$delta" -lt "$best" ]]
        then
            best=$delta
        fi
        if [[ "$delta" -eq 0 ]]
        then
            break
        fi
    done

    echo "$best"
}

# Refusal probe for a query that HAS a `WHERE`: no oracle may report a mismatch on it, and none may
# report one to the client either.
no_mismatch()
{
    local label="$1"
    local delta

    delta=$(smallest_delta "$2" ASTFuzzerOracleMismatches)

    if [[ "$delta" == "invalid" ]]
    then
        echo "$label: not a valid query"
    elif [[ "$delta" -ne 0 ]]
    then
        echo "$label: mismatched $delta times"
    elif grep -q AST_FUZZER_ORACLE_MISMATCH "$ERR"
    then
        echo "$label: mismatch reported to the client"
    else
        echo "$label: no mismatch"
    fi
}

# Refusal probe for a query no oracle may check at all, not even the predicate-rewriting one - which
# is the case only when the call is written plainly, because then it is also a call to a function
# whose result is not reproducible, and every oracle screens for those too.
not_checked()
{
    local label="$1"
    local delta

    delta=$(smallest_delta "$2" ASTFuzzerOracleChecks)

    if [[ "$delta" == "invalid" ]]
    then
        echo "$label: not a valid query"
    elif [[ "$delta" -ne 0 ]]
    then
        echo "$label: checked $delta times"
    else
        echo "$label: not checked"
    fi
}

# Positive control: the query IS checked, so a refusal widened into "skip anything carrying an APPLY"
# - or into "skip anything mentioning arrayJoin anywhere, subqueries included" - reddens here instead
# of silently deleting oracle coverage. Retried until the counter moves, because a single mutation can
# occasionally break oracle eligibility for an unrelated reason. The budget is far below the point
# where the loop alone could exhaust the per-test time limit.
still_checked()
{
    local label="$1"
    local query="$2"
    local before
    local after

    before=$(get_counter ASTFuzzerOracleChecks)
    after=$before
    for _ in $(seq 1 10)
    do
        after=$(run_fuzzed_rounds "$query" ASTFuzzerOracleChecks)
        if [[ "$after" -gt "$before" ]]
        then
            break
        fi
    done

    if [[ "$after" -gt "$before" ]]
    then
        echo "$label: checked"
    else
        echo "$label: never checked, counter stayed at $after"
    fi
}

# One probe per spelling of the hidden call, so that a spelling which stops being refused fails on its
# own rather than being covered by a sibling in the same query.
no_mismatch "lambda arrayJoin" \
    "SELECT * APPLY (x -> arrayJoin([x, x + 1])) FROM oracle_apply_array_join WHERE i > 1"
no_mismatch "lambda unnest" \
    "SELECT * APPLY (x -> unnest([x, x + 1])) FROM oracle_apply_array_join WHERE i > 1"
no_mismatch "bare function name of a UDF" \
    "SELECT * APPLY $UDF FROM oracle_apply_array_join WHERE i > 1"
no_mismatch "COLUMNS matcher" \
    "SELECT COLUMNS('i') APPLY (x -> arrayJoin([x, x + 1])) FROM oracle_apply_array_join WHERE i > 1"
no_mismatch "qualified asterisk" \
    "SELECT oracle_apply_array_join.* APPLY (x -> arrayJoin([x, x + 1])) FROM oracle_apply_array_join WHERE i > 1"
no_mismatch "second of two chained transformers" \
    "SELECT * APPLY (x -> x + 1) APPLY (y -> arrayJoin([y, y + 1])) FROM oracle_apply_array_join WHERE i > 1"

still_checked "transformer without arrayJoin" \
    "SELECT * APPLY (x -> x + 1) FROM oracle_apply_array_join WHERE i > 1"

# An `arrayJoin` inside a subquery in the lambda has a scope of its own and does not multiply the
# outer rows, so the query stays checkable - and it agrees.
still_checked "arrayJoin scoped inside a subquery" \
    "SELECT * APPLY (x -> x + (SELECT sum(arrayJoin([1, 2])) FROM oracle_apply_array_join)) FROM oracle_apply_array_join WHERE i > 1"
no_mismatch "arrayJoin scoped inside a subquery" \
    "SELECT * APPLY (x -> x + (SELECT sum(arrayJoin([1, 2])) FROM oracle_apply_array_join)) FROM oracle_apply_array_join WHERE i > 1"

# The spelling that was already refused stays refused: nothing about the plainly written call changes
# here, and this is the one query shape whose check counter can be asserted outright.
not_checked "call written in the select list" \
    "SELECT arrayJoin([i, i + 1]) FROM oracle_apply_array_join WHERE i > 1"

$CLICKHOUSE_CLIENT --query "
    DROP FUNCTION $UDF;
    DROP TABLE oracle_apply_array_join;
"
rm -f "$ERR"
