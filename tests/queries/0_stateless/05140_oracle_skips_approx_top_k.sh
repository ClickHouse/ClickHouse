#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: the proof event `ASTFuzzerOracleChecks` is server-global, and the assertions
#              below require it to stay put, so no other test may run oracle checks against
#              the same server meanwhile.
#
# `approx_top_k` / `approx_top_sum` keep a bounded counter table (the space-saving algorithm),
# so which elements survive - and the counts reported for them - depend on the order the values
# arrive in and on how the partial states are merged. The oracles compare results for exact row
# equality, so `QueryOracleChecker` has to reject a query naming such an aggregate rather than
# check it. `QueryFuzzer`'s swap list offers `approx_top_k` for any single-argument aggregate,
# which is how a false `TLP Aggregate oracle mismatch!` reached master CI:
# https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?REF=master&sha=6c335833c268d3c52d83b47abf4c2f807251e716&name_0=MasterCI&name_1=Stateless%20tests%20%28amd_debug%2C%20sequential%29
#
# The alias spellings that only a lookup through `AggregateFunctionFactory` can reject live in
# 05141_oracle_skips_aggregate_aliases, as two files each comfortably under the 180s per-test
# limit: every assertion here costs a separate `clickhouse-client` invocation, and in the
# private `s3 storage, meta in keeper` configuration one such invocation costs seconds rather
# than milliseconds, so the run time of these tests is set by how many of them there are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_approx_top;
    CREATE TABLE oracle_approx_top (v Int64) ENGINE = MergeTree ORDER BY v;
    -- All values distinct and far more numerous than the default k = 10, so every partition of
    -- a TLP rewrite contributes its own equally-frequent candidates: were the oracle to run
    -- here, the merged top-k would legitimately differ from the directly computed one.
    INSERT INTO oracle_approx_top SELECT number FROM numbers(200);
"

# The oracle rejects any query reading `system.*`, so reading the counter can never move it.
get_counter()
{
    $CLICKHOUSE_CLIENT --query "SELECT toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerOracleChecks'"
}

# `send_logs_level = 'fatal'` suppresses the expected error-level log lines from random
# mutations that produce valid-but-nonsense queries (see 04256_04250). No `FORMAT Null` on the
# fuzzed query: the oracle skips queries carrying an explicit FORMAT clause, which would make
# every assertion below vacuous - the rows a round prints are simply not the line the caller
# reads back.
#
# All rounds of one probe are sent as a single multi-statement invocation, and the counter
# reading taken right after them travels in the same one - it is printed last, so the caller
# reads it off the tail. A statement's oracle checks run in that statement's own finish
# callback, so by the time the trailing `system.events` read executes, all three rounds have
# been checked. Each round is still fuzzed and oracle-checked on its own - a batch of three
# copies of a checkable query moves the counter by three - so batching only removes client
# start-up cost, which is what dominates the run time (see the header).
#
# The counter cannot come from the client's own `--print-profile-events` output instead:
# measured, that prints the query's ~75 events but never `ASTFuzzerOracleChecks`, because
# `QueryOracleChecker` runs from the query's finish callback while `TCPHandler` sends the last
# `ProfileEvents` packet inside `processOrdinaryQuery`, i.e. before `io.onFinish()`. The
# increment lands after the client's last packet, so only the server-global counter sees it.
#
# `SETTINGS ast_fuzzer_runs = 0` on that trailing read keeps the fuzzer off it. The oracle
# would skip it in any case - it rejects any query reading `system.*`, which is why reading
# the counter can never move it - but there is no reason to spend a mutation on it.
#
# `--ignore-error` is what keeps the rounds independent of each other. Without it the client
# abandons the rest of a multi-statement batch as soon as one statement raises
# (`have_error && !ignore_error` in `ClientBase::executeMultiQuery`), so a first round whose
# mutation happened to produce an invalid query would silently cancel the two rounds that
# exist precisely to cover that case, and the probe could pass vacuously off a single unlucky
# attempt - the very thing the rounds are insurance against. It also lets the trailing counter
# read still happen when a round raised.
run_fuzzed_rounds()
{
    local query="$1"

    $CLICKHOUSE_CLIENT --ignore-error --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        $query
        $query
        $query
        SELECT toInt64(sum(value)) FROM system.events
        WHERE event = 'ASTFuzzerOracleChecks' SETTINGS ast_fuzzer_runs = 0;
    " 2>/dev/null | tail -n 1
}

# One probe per spelling: each query names exactly ONE of the aggregates under test, so a
# probe fails on its own the moment that spelling stops being rejected - a query mixing
# several unsafe aggregates would stay oracle-unsafe (and the test green) even if all but
# one of them were dropped from the denylist.
# The spelling is repeated three times within the query because `QueryFuzzer` replaces an
# aggregate node with a random name from its swap list once in 30 nodes: with a single
# occurrence a swap to a deterministic aggregate would let the oracle run and turn the
# assertion into a false failure, while losing all three occurrences in one round is
# vanishingly unlikely. Aliases are not swapped at all (the swap list matches on the
# canonical prefix), so for them the repetition is merely harmless.
#
# Three rounds, not more. Rounds buy only insurance against a round in which the mutation
# happens to make the query ineligible for an unrelated reason and the probe passes
# vacuously; that risk falls off geometrically, so three is already plenty. They do not add
# detection power - a missing denylist entry moves the counter on the very first round - and
# each extra round costs real time and one more chance of the false failure above. Keep this
# number small: at eight rounds the whole test took ~204s under `amd_asan_ubsan` and tripped
# the 180s per-test limit. The rounds are now batched into one client invocation, so raising
# them is cheaper than it was - that is not a reason to raise them.
probe()
{
    local label="$1"
    local aggregates="$2"
    local before
    local after

    # A zero delta on its own does not prove the oracle gate rejected the spelling: it holds
    # just as well when the query never reached `QueryOracleChecker` at all. If, say,
    # `approx_top_count` stopped resolving as an aggregate, the query would fail before the
    # gate and the probe would still print `not checked`. So first run the very same query
    # with no fuzzer and no oracle and require it to succeed - that separates "unsafe
    # spelling was skipped" from "this spelling is broken". A `FORMAT Null` is fine here
    # precisely because the oracle is off in this run, so it cannot make the check below
    # vacuous, and the counter cannot move - which is also why the `before` snapshot can be
    # taken in the same invocation. Reading it per probe rather than carrying each probe's
    # reading over as the next one's baseline costs nothing - either way it is two
    # invocations per probe - and keeps every probe self-contained, which is worth more now
    # that the probes are spread over two files with positive controls in between.
    if ! before=$($CLICKHOUSE_CLIENT --query "
        SELECT $aggregates FROM oracle_approx_top WHERE v > 5 FORMAT Null;
        SELECT toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerOracleChecks';
    ")
    then
        echo "$label is not a valid query"
        return
    fi

    after=$(run_fuzzed_rounds "SELECT $aggregates FROM oracle_approx_top WHERE v > 5;")

    if [[ "$after" -eq "$before" ]]
    then
        echo "$label not checked"
    else
        echo "$label checked $((after - before)) times"
    fi
}

probe "approx_top_k" "approx_top_k(v), approx_top_k(v + 1), approx_top_k(v * 2)"
probe "approx_top_sum" "approx_top_sum(v, 1), approx_top_sum(v + 1, 1), approx_top_sum(v * 2, 1)"

# `approx_top_count` is an alias of `approx_top_k` and must be rejected as well. The backstop
# set does not name it, so this only holds once the name is resolved through the aggregate
# function factory. The other alias spellings are covered by 05141.
probe "approx_top_count" "approx_top_count(v), approx_top_count(v + 1), approx_top_count(v * 2)"

# Positive control: the same query shape over exact, merge-order-independent aggregates IS
# checked. This proves the counter is live under these settings, so every probe above is
# the gate rejecting one unsafe spelling and not the oracle ignoring this query shape
# altogether. Retried until the counter moves, because a single mutation can occasionally
# break oracle eligibility for an unrelated reason (see 04658). The retry budget is bounded
# well below the point where the loop alone could exhaust the 180s per-test limit: it
# normally succeeds on the first round, and if it genuinely never fires it is far better to
# report that below than to be killed as a timeout.
positive_control()
{
    local label="$1"
    local aggregates="$2"
    local before
    local after

    before=$(get_counter)
    after=$before
    for _ in $(seq 1 10)
    do
        after=$(run_fuzzed_rounds "SELECT $aggregates FROM oracle_approx_top WHERE v > 5;")
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

positive_control "exact aggregates" "count(), min(v), max(v)"

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_approx_top"
