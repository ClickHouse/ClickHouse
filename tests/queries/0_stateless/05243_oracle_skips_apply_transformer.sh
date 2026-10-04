#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: the proof events below are server-global, and the assertions require them to
#              stay put, so no other test may run oracle checks against the same server
#              meanwhile - and 05097_ast_fuzzer_oracle_apply_aggregate, which runs oracle
#              checks over this very shape, is not itself tagged `no-parallel`.
# long: the cost is one fuzzed execution per gate probed, plus the oracles that accept it, so it
#       scales with the number of gates rather than with any data this test writes, and each of
#       the gates below is covered by this file alone. 05099_ast_fuzzer_oracle_view_definition
#       carries the tag for the same reason.
#
# Third companion of 05140_oracle_skips_approx_top_k and 05141_oracle_skips_aggregate_aliases.
# Those two cover which NAMES the oracle's backstop set and the factory lookup must reject.
# This file covers where the checker has to LOOK in the first place: a function applied by a column
# transformer, and anything a subquery hidden in that transformer brings with it - each of which the
# oracle already refuses when the same construct is written in the select list directly.
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

# One shape per probe, so a probe fails exactly when the reason it names stops being seen. A query
# carrying several of them would stay oracle-unsafe - and the test green - even if all but one of
# them regressed.
#
# Refusals first: the oracle must skip each of these, so its check counter must not move.
refuse_labels=()
refuse_lists=()
refuse()
{
    refuse_labels+=("$1")
    refuse_lists+=("$2")
}

# The shape observed failing in CI: `any` is in the backstop set, but it lives in `func_name`,
# so only a screen that reads that member can reject it. `DISTINCT` is part of the observed
# query and is deliberately kept - it is one of the clauses `checkIdentityWhere` admits.
refuse "any" "DISTINCT * APPLY any"

# The same hazard spelled as a lambda, which is stored in `lambda` and is likewise not a child.
# Without the recursion into that member this query stays checkable even once `func_name` is
# screened, so this probe is what distinguishes the two members.
refuse "lambda any" "* APPLY (x -> any(x))"

# A parameterized spelling - `APPLY (quantile(0.9))` is the `func_name` + `parameters` form the
# node's own header calls "Case 1". `quantile` is in the backstop set, so this pins that the
# member is read for the parameterized form too, where the name is not the whole expression.
refuse "quantile" "* APPLY (quantile(0.9))"

# And a shape where ONLY the `parameters` subtree is unsafe: `uniqUpTo` is exact and
# order-independent, is absent from the backstop set, and resolves through no alias, so
# nothing about the name can reject this query - but `randConstant` is in the set, and it is
# reachable here because an aggregate parameter merely has to be a CONSTANT expression, not a
# literal (`randConstant() % 2 + 1` folds; a per-row `rand()` is refused with
# `Code: 36 ... expected to have constant value`). Without the recursion into `parameters` the
# oracle compares two executions that drew a different constant.
refuse "randConstant parameter" "* APPLY (uniqUpTo(randConstant() % 2 + 1))"

# The applied function is an expression, so it can hold a subquery, and a subquery can name any
# relation and carry any clause or SETTINGS of its own. Every probe below is a query the oracle
# already refuses when the same construct is written in the select list directly - the reasons are
# in the skip messages of `QueryOracleChecker::check` - and each pins a different one of those
# reasons. Hiding the construct in the transformer must not change the verdict. Before this was
# screened, the view spelling raised a false mismatch, not merely a needless check.
refuse "system table in lambda" "* APPLY (x -> plus(toInt32(x), (SELECT toInt32(count()) FROM system.events)))"
refuse "stored definition in lambda" "* APPLY (x -> plus(toInt32(x), (SELECT r FROM oracle_apply_nd_view)))"
refuse "inline settings in lambda" \
    "* APPLY (x -> plus(toInt32(x), (SELECT toInt32(count()) FROM oracle_apply_screen
                                     SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break')))"
# A separate probe from the one above: thread counts are the one group of inline settings the
# oracle strips rather than refuses, so only the nested-clause screen rejects this query.
refuse "thread settings in lambda" \
    "* APPLY (x -> plus(toInt32(x), (SELECT toInt32(count()) FROM oracle_apply_screen
                                     SETTINGS max_threads = 1)))"
refuse "with fill in lambda" \
    "* APPLY (x -> plus(toInt32(x), (SELECT toInt32(count()) FROM
                                     (SELECT i FROM oracle_apply_screen ORDER BY i WITH FILL FROM 1 TO 6))))"
# Unordered window functions are the other thing the Identity WHERE and subquery-wrap oracles have
# only a screen of their own to defend them against: which row gets which number is not fixed.
refuse "unordered window in lambda" "* APPLY (x -> toInt32(row_number() OVER ()))"
refuse "asof join in lambda" \
    "* APPLY (x -> plus(toInt32(x), (SELECT toInt32(count()) FROM oracle_apply_screen AS a
                                     ASOF JOIN oracle_apply_screen AS b ON a.d = b.d AND a.i > b.i)))"

# Positive controls, reached through the very same members: a deterministic `APPLY` must still be
# checked. Without them every probe above would be satisfied just as well by a screen that
# rejected every `APPLY` outright - which would silently cost the oracle all of its coverage of
# column transformers - or, for the subquery probes, by one that refused every transformer
# carrying a subquery.
check_labels=()
check_lists=()
expect_check()
{
    check_labels+=("$1")
    check_lists+=("$2")
}

# `toString` is deterministic and `FunctionFactory` reports it as such, so the screen must let
# it through even though it is reached through exactly the same `func_name` member as `any`.
# 05097 asserts the same property on the result rows; here it is asserted on the screen.
expect_check "toString" "* APPLY toString"
expect_check "ordered window in lambda" "* APPLY (x -> toInt32(row_number() OVER (ORDER BY i)))"
expect_check "deterministic subquery in lambda" \
    "* APPLY (x -> plus(toInt32(x), (SELECT toInt32(count()) FROM oracle_apply_screen)))"

# The oracle rejects any query reading `system.*`, so reading its counter can never move it. The
# read is tagged so that it can be told apart from the probe queries' own result rows when several
# of them travel in one invocation.
mark='@@oracle@@'
counter_read="SELECT '$mark', toInt64(sum(value)) FROM system.events
              WHERE event = 'ASTFuzzerOracleChecks' SETTINGS ast_fuzzer_runs = 0;"

read_counter()
{
    $CLICKHOUSE_CLIENT --query "$counter_read" 2>/dev/null \
        | awk -F'\t' -v m="$mark" '$1 == m { print $2 }'
}

# Three rounds per refusal probe. Rounds are insurance against a round whose mutation leaves the
# query ineligible for an unrelated reason, letting a probe pass vacuously; that is measured rather
# than theoretical - against a server with none of these screens, a single round leaves several of
# the probes below unable to redden at all. Beyond that they add no detection power, since a blind
# screen moves the counter on the very first round, and a round that IS checked is the most
# expensive thing in this file - which is why a control takes one: it only needs the counter to
# move once, and it already retries until it does.
rounds()
{
    local query="SELECT $1 FROM oracle_apply_screen WHERE i > 1$2;"
    local out=""
    local n

    for (( n = 0; n < $3; n++ ))
    do
        out+="$query "
    done
    echo "$out"
}

# `send_logs_level = 'fatal'` suppresses the expected error-level log lines from random mutations
# that produce valid-but-nonsense queries (see 04256_04250). No `FORMAT Null` on a fuzzed query:
# the oracle skips queries carrying an explicit FORMAT clause, which would make every assertion
# below vacuous. `--ignore-error` keeps the statements independent - without it the client
# abandons the rest of the batch as soon as one mutation produces an invalid query.
#
# A statement's oracle checks run in that statement's own finish callback, so a counter read placed
# after a probe's rounds already sees all of them. Every probe therefore travels in one invocation -
# one read before the first, one after each - and the caller takes the deltas between consecutive
# reads. Client startup, not the fuzzed rounds, is what this test spends its time on, and the
# sibling 05141 tripped the 180s per-test limit on invocation count alone. The counter cannot come
# from the client's own `--print-profile-events` output: `QueryOracleChecker` runs from the query's
# finish callback, after `TCPHandler` has sent the last `ProfileEvents` packet, so only the
# server-global counter sees the increment.
fuzz_and_read()
{
    $CLICKHOUSE_CLIENT --ignore-error --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        $1
    " 2>/dev/null | awk -F'\t' -v m="$mark" '$1 == m { print $2 }'
}

# A zero delta on its own does not prove the screen rejected a shape: it holds just as well when
# the query never reached `QueryOracleChecker` at all. So the fixture invocation also runs every
# refusal probe once with the fuzzer and the oracle off, and requires it to parse and execute. It
# stops at the first failure, so the tags it printed are the prefix that ran and the first missing
# one names the broken probe. A `FORMAT Null` is fine here precisely because the oracle is off, so
# it cannot make the assertions below vacuous, and the counter cannot move.
#
# Two columns of different types, so `*` expands to more than one argument for the applied function
# and the expansion is not a degenerate single-column case.
fixture_sql="
    SET ast_fuzzer_runs = 0;
    SET ast_fuzzer_oracle = 0;
    DROP TABLE IF EXISTS oracle_apply_screen;
    DROP VIEW IF EXISTS oracle_apply_nd_view;
    CREATE TABLE oracle_apply_screen (i Int32, d Date) ENGINE = MergeTree ORDER BY i;
    INSERT INTO oracle_apply_screen VALUES (1, '2020-01-01'), (2, '2020-01-02'), (3, '2020-01-03');
    CREATE VIEW oracle_apply_nd_view AS SELECT toInt32(rand() % 1000000) AS r;
"
for idx in "${!refuse_labels[@]}"
do
    fixture_sql+="SELECT ${refuse_lists[$idx]} FROM oracle_apply_screen WHERE i > 1 FORMAT Null;
                  SELECT '$mark', 1;"
done
valid=$($CLICKHOUSE_CLIENT --query "$fixture_sql" 2>/dev/null \
    | awk -F'\t' -v m="$mark" '$1 == m { n++ } END { print n + 0 }')

if [[ "$valid" -lt "${#refuse_labels[@]}" ]]
then
    echo "${refuse_labels[$valid]} is not a valid query"
fi

# One sweep for both groups, refusals first, so that the whole file costs three invocations.
sweep_sql="$counter_read"
for idx in "${!refuse_labels[@]}"
do
    sweep_sql+="$(rounds "${refuse_lists[$idx]}" "" 3)$counter_read"
done
for idx in "${!check_labels[@]}"
do
    sweep_sql+="$(rounds "${check_lists[$idx]}" " ORDER BY i" 1)$counter_read"
done

probes=$(( ${#refuse_labels[@]} + ${#check_labels[@]} ))
readarray -t counts < <(fuzz_and_read "$sweep_sql")
if [[ "${#counts[@]}" -ne $(( probes + 1 )) ]]
then
    # Reported, not worked around: an unexpected number of reads means the deltas below cannot be
    # trusted, and a test whose measurement broke must say so rather than print a verdict.
    echo "sweep returned ${#counts[@]} counter reads, expected $(( probes + 1 ))" >&2
fi

deltas=()
for (( idx = 0; idx < probes; idx++ ))
do
    deltas+=( "$(( ${counts[$((idx + 1))]:-1} - ${counts[$idx]:-0} ))" )
done

# The two outcomes are not symmetric, so a non-zero delta is re-measured while a zero delta is
# final. A round can mutate the hazard OUT of the query - the select-list fuzzer may replace an
# element outright, for instance with a virtual column reference - and the oracle then
# legitimately checks a query with nothing left to reject. A screen that stopped reading the
# member, by contrast, moves the counter on every attempt.
for idx in "${!refuse_labels[@]}"
do
    delta="${deltas[$idx]}"
    for _ in 1 2
    do
        [[ "$delta" -eq 0 ]] && break
        before=$(read_counter)
        after=$(fuzz_and_read "$(rounds "${refuse_lists[$idx]}" "" 3)$counter_read" | tail -n 1)
        delta=$(( after - before ))
    done

    if [[ "$delta" -eq 0 ]]
    then
        echo "${refuse_labels[$idx]} not checked"
    else
        echo "${refuse_labels[$idx]} checked $delta times"
    fi
done

# Retried until the counter moves, because a single mutation can occasionally break oracle
# eligibility for an unrelated reason (see 04658). The budget is bounded well below the point
# where the loop alone could exhaust the 180s per-test limit.
for idx in "${!check_labels[@]}"
do
    delta="${deltas[$(( ${#refuse_labels[@]} + idx ))]}"
    for _ in $(seq 1 9)
    do
        [[ "$delta" -gt 0 ]] && break
        before=$(read_counter)
        after=$(fuzz_and_read "$(rounds "${check_lists[$idx]}" " ORDER BY i" 1)$counter_read" | tail -n 1)
        delta=$(( after - before ))
    done

    if [[ "$delta" -gt 0 ]]
    then
        echo "${check_labels[$idx]} checked"
    else
        echo "${check_labels[$idx]} never checked"
    fi
done

$CLICKHOUSE_CLIENT --query "DROP VIEW oracle_apply_nd_view; DROP TABLE oracle_apply_screen"
