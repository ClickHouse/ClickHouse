#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: the proof events below are server-global, and the assertions require them to
#              stay put, so no other test may run oracle checks against the same server
#              meanwhile - and 05097_ast_fuzzer_oracle_apply_aggregate, which runs oracle
#              checks over this very shape, is not itself tagged `no-parallel`.
#
# Third companion of 05140_oracle_skips_approx_top_k and 05141_oracle_skips_aggregate_aliases.
# Those two cover which NAMES the oracle's backstop set and the factory lookup must reject.
# This file covers where the checker has to LOOK for the name in the first place.
#
# `ASTColumnsApplyTransformer` stores the applied function as a bare `String func_name` (or an
# `ASTPtr lambda`), and neither it, `parameters` nor `lambda` is registered among the node's
# children - `Parsers/ASTColumnsTransformers.h` copies both members by hand in `clone()`
# instead of calling `cloneChildren()`, which is also why the node needs its own
# `updateTreeHashImpl`. A screen that walks `children` and inspects only `ASTFunction` nodes
# therefore never sees them, so `SELECT * APPLY any` was NOT rejected by
# `hasNonDeterministicFunctions` even though `any` is listed in its backstop set. All nine
# oracles share that screen, and `checkIdentityWhere` admits DISTINCT/GROUP BY/aggregates
# with only that screen and the window screen to defend it, so it happily compared two
# executions of a query whose value `any` picks arbitrarily - a false
# `AST_FUZZER_ORACLE_MISMATCH` on unrelated PRs.
#
# The companion fix to `scanAggregatesSafe` taught the AGGREGATE screen about this node; these
# probes pin the NON-DETERMINISM screen, which is a different predicate in the same file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two columns of different types, so `*` expands to more than one argument for the applied
# function and the expansion is not a degenerate single-column case.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_apply_screen;
    CREATE TABLE oracle_apply_screen (i Int32, d Date) ENGINE = MergeTree ORDER BY i;
    INSERT INTO oracle_apply_screen VALUES (1, '2020-01-01'), (2, '2020-01-02'), (3, '2020-01-03');
"

# The oracle rejects any query reading `system.*`, so reading a counter can never move it.
get_counter()
{
    local event="${1:-ASTFuzzerOracleChecks}"

    $CLICKHOUSE_CLIENT --query "SELECT toInt64(sum(value)) FROM system.events WHERE event = '$event'"
}

# `send_logs_level = 'fatal'` suppresses the expected error-level log lines from random
# mutations that produce valid-but-nonsense queries (see 04256_04250). No `FORMAT Null` on the
# fuzzed query: the oracle skips queries carrying an explicit FORMAT clause, which would make
# every assertion below vacuous.
#
# All rounds of one probe are sent as a single multi-statement invocation, and the counter
# reading taken right after them travels in the same one - it is printed last, so the caller
# reads it off the tail. A statement's oracle checks run in that statement's own finish
# callback, so by the time the trailing `system.events` read executes, all rounds have been
# checked. The counter cannot come from the client's own `--print-profile-events` output:
# `QueryOracleChecker` runs from the query's finish callback, after `TCPHandler` has sent the
# last `ProfileEvents` packet, so only the server-global counter sees the increment.
#
# `--ignore-error` keeps the rounds independent: without it the client abandons the rest of a
# multi-statement batch as soon as one statement raises, so a first round whose mutation
# produced an invalid query would cancel the remaining rounds and the trailing counter read.
run_fuzzed_rounds()
{
    local query="$1"
    local event="${2:-ASTFuzzerOracleChecks}"

    $CLICKHOUSE_CLIENT --ignore-error --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        $query
        $query
        $query
        SELECT toInt64(sum(value)) FROM system.events
        WHERE event = '$event' SETTINGS ast_fuzzer_runs = 0;
    " 2>/dev/null | tail -n 1
}

# One probe per spelling: each query carries exactly ONE of the transformer shapes under test,
# so a probe fails on its own the moment that shape stops being rejected. A query mixing
# several would stay oracle-unsafe (and the test green) even if all but one of them regressed.
#
# Three rounds, not more. Rounds are insurance against a round whose mutation makes the query
# ineligible for an unrelated reason, letting the probe pass vacuously; that risk falls off
# geometrically, so three is plenty. They add no detection power - a blind screen moves the
# counter on the very first round - and each extra round costs real time. Keep this number
# small: the sibling 05141 tripped the 180s per-test limit at eight rounds under
# `amd_asan_ubsan`.
probe()
{
    local label="$1"
    local select_list="$2"
    local before
    local after
    local delta

    # A zero delta on its own does not prove the screen rejected the shape: it holds just as
    # well when the query never reached `QueryOracleChecker` at all. So first run the very same
    # query with no fuzzer and no oracle and require it to succeed - that separates "unsafe
    # shape was skipped" from "this shape is broken". A `FORMAT Null` is fine here precisely
    # because the oracle is off in this run, so it cannot make the check below vacuous, and the
    # counter cannot move - which is also why the `before` snapshot can be taken in the same
    # invocation.
    #
    # The gate's stderr is discarded rather than left to reach the test's stderr:
    # `clickhouse-test` fails a test whose stderr is non-empty regardless of stdout, so letting
    # a server error through would turn the diagnosis this branch prints into an unconditional
    # test failure.
    if ! before=$($CLICKHOUSE_CLIENT --query "
        SELECT $select_list FROM oracle_apply_screen WHERE i > 1 FORMAT Null;
        SELECT toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerOracleChecks';
    " 2>/dev/null)
    then
        echo "$label is not a valid query"
        return
    fi

    # The two outcomes are not symmetric, so read the counter until it agrees with itself. A round
    # can mutate the hazard OUT of the query - the select-list fuzzer may replace an element
    # outright, for instance with a virtual column reference - and the oracle then checks a query
    # with nothing left to reject. So a non-zero delta is inconclusive while a zero delta is not,
    # and a screen that stopped reading the member moves the counter on every attempt.
    for _ in $(seq 1 3)
    do
        after=$(run_fuzzed_rounds "SELECT $select_list FROM oracle_apply_screen WHERE i > 1;")
        delta=$((after - before))
        if [[ "$delta" -eq 0 ]]
        then
            break
        fi
        before=$(get_counter)
    done

    if [[ "$delta" -eq 0 ]]
    then
        echo "$label not checked"
    else
        echo "$label checked $delta times"
    fi
}

# The shape observed failing in CI: `any` is in the backstop set, but it lives in `func_name`,
# so only a screen that reads that member can reject it. `DISTINCT` is part of the observed
# query and is deliberately kept - it is one of the clauses `checkIdentityWhere` admits.
probe "any" "DISTINCT * APPLY any"

# The same hazard spelled as a lambda, which is stored in `lambda` and is likewise not a child.
# Without the recursion into that member this query stays checkable even once `func_name` is
# screened, so this probe is what distinguishes the two members.
probe "lambda any" "* APPLY (x -> any(x))"

# A parameterized spelling - `APPLY (quantile(0.9))` is the `func_name` + `parameters` form the
# node's own header calls "Case 1". `quantile` is in the backstop set, so this pins that the
# member is read for the parameterized form too, where the name is not the whole expression.
probe "quantile" "* APPLY (quantile(0.9))"

# And a shape where ONLY the `parameters` subtree is unsafe: `uniqUpTo` is exact and
# order-independent, is absent from the backstop set, and resolves through no alias, so
# nothing about the name can reject this query - but `randConstant` is in the set, and it is
# reachable here because an aggregate parameter merely has to be a CONSTANT expression, not a
# literal (`randConstant() % 2 + 1` folds; a per-row `rand()` is refused with
# `Code: 36 ... expected to have constant value`). Without the recursion into `parameters` the
# oracle compares two executions that drew a different constant.
probe "randConstant parameter" "* APPLY (uniqUpTo(randConstant() % 2 + 1))"

# Positive control: a deterministic `APPLY` must still be checked. Without it every probe
# above would be satisfied just as well by a screen that rejected every `APPLY` outright -
# which would silently cost the oracle all of its coverage of column transformers. Retried
# until the counter moves, because a single mutation can occasionally break oracle eligibility
# for an unrelated reason (see 04658). The retry budget is bounded well below the point where
# the loop alone could exhaust the 180s per-test limit.
positive_control()
{
    local label="$1"
    local select_list="$2"
    local event="${3:-ASTFuzzerOracleChecks}"
    local before
    local after

    before=$(get_counter "$event")
    after=$before
    for _ in $(seq 1 10)
    do
        after=$(run_fuzzed_rounds "SELECT $select_list FROM oracle_apply_screen WHERE i > 1 ORDER BY i;" "$event")
        if [[ "$after" -gt "$before" ]]
        then
            break
        fi
    done

    if [[ "$after" -gt "$before" ]]
    then
        echo "$label checked"
    else
        echo "$label never checked: counter stayed at $after"
    fi
}

# `toString` is deterministic and `FunctionFactory` reports it as such, so the screen must let
# it through even though it is reached through exactly the same `func_name` member as `any`.
# 05097 asserts the same property on the result rows; here it is asserted on the screen.
positive_control "toString" "* APPLY toString"

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_apply_screen"
