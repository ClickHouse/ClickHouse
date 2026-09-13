#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: the proof event `ASTFuzzerOracleChecks` is server-global, and the assertions
#              below require it to stay put, so no other test may run oracle checks against
#              the same server meanwhile.
#
# Companion of 05140_oracle_skips_approx_top_k, which covers the `approx_top_*` family itself.
# This file covers the part that only a lookup through `AggregateFunctionFactory` can get
# right: an alias spelling of an oracle-unsafe aggregate must be rejected even though the
# backstop set in `QueryOracleChecker` names none of these under its alias, and - just as
# importantly - an alias of a *safe* aggregate must stay checkable, so the resolution cannot
# simply reject everything that turns out to be an alias.
#
# The two files are split rather than one because every assertion costs a separate
# `clickhouse-client` invocation, and in the private `s3 storage, meta in keeper`
# configuration one such invocation costs seconds rather than milliseconds; as a single file
# the whole set ran right at the 180s per-test limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_alias_agg;
    CREATE TABLE oracle_alias_agg (v Int64) ENGINE = MergeTree ORDER BY v;
    -- All values distinct, so a TLP rewrite splits them into partitions that each contribute
    -- their own equally-frequent candidates: were the oracle to run over the approximate
    -- aggregates below, the merged result would legitimately differ from the direct one.
    INSERT INTO oracle_alias_agg SELECT number FROM numbers(200);
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
    # `anova` stopped resolving as an alias, the query would fail before the gate and the
    # probe would still print `not checked`. So first run the very same query with no fuzzer
    # and no oracle and require it to succeed - that separates "unsafe spelling was skipped"
    # from "this spelling is broken". A `FORMAT Null` is fine here precisely because the
    # oracle is off in this run, so it cannot make the check below vacuous, and the counter
    # cannot move - which is also why the `before` snapshot can be taken in the same
    # invocation. Reading it per probe rather than carrying each probe's reading over as the
    # next one's baseline costs nothing - either way it is two invocations per probe - and
    # keeps every probe self-contained, which is worth more now that the probes are spread
    # over two files with positive controls in between.
    if ! before=$($CLICKHOUSE_CLIENT --query "
        SELECT $aggregates FROM oracle_alias_agg WHERE v > 5 FORMAT Null;
        SELECT toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerOracleChecks';
    ")
    then
        echo "$label is not a valid query"
        return
    fi

    after=$(run_fuzzed_rounds "SELECT $aggregates FROM oracle_alias_agg WHERE v > 5;")

    if [[ "$after" -eq "$before" ]]
    then
        echo "$label not checked"
    else
        echo "$label checked $((after - before)) times"
    fi
}

# `min_by` is an alias of `argMin`, `array_agg` of `groupArray`, `medianTDigest` of the
# approximate `quantileTDigest`, `anova` of `analysisOfVariance`, and `array_concat_agg` of
# `groupArrayArray` - which is itself `groupArray` plus an `Array` combinator, so the
# expansion has to be stripped in turn.
probe "min_by" "min_by(v, v), min_by(v + 1, v), min_by(v * 2, v)"
probe "array_agg" "array_agg(v), array_agg(v + 1), array_agg(v * 2)"
probe "array_concat_agg" "array_concat_agg([v]), array_concat_agg([v + 1]), array_concat_agg([v * 2])"
probe "medianTDigest" "medianTDigest(v), medianTDigest(v + 1), medianTDigest(v * 2)"
# `anova`'s group argument is written `toUInt8(v % 3)` rather than `(v % 3)::UInt8` on purpose.
# A `CAST` node is itself a fuzzer target - `QueryFuzzer` rewrites the target type of a
# `CAST` / `_CAST` / `accurateCast*` call to a random type once in 30 nodes (`cast_functions`) -
# and a randomly retyped group argument makes `anova` fail to resolve, so the round errors out
# before it ever reaches the oracle gate and the probe passes without having tested anything.
# With three occurrences that happens often enough to matter: measured against a binary with no
# alias resolution, the probe detects the regression in 8 of 12 runs spelled `toUInt8` and in
# only 2 of 12 spelled with the cast. The rewrite can only ever turn a round into an error,
# never into a checked query, so it was never a false-failure risk - just a blind spot.
probe "anova" "anova(v, toUInt8(v % 3)), anova(v + 1, toUInt8(v % 3)), anova(v * 2, toUInt8(v % 3))"

# One spelling that carries a combinator on top of the alias, which is what forces the alias
# lookup to run at every combinator-stripping stage rather than once on the original name:
# `min_byOrNull` is not a registered name at all (`system.functions` knows `min_by`, not
# `min_byOrNull`), so the factory can only recognise it once the `OrNull` suffix has come off.
# All the probes above use the raw alias spelling and would stay green under a checker that
# resolved the alias exactly once, up front - including `array_concat_agg`, which only proves
# that the *expansion* is stripped afterwards. Measured: with the resolution moved to a single
# up-front lookup, every probe above stays green while this one turns checkable.
# `*OrNull` rather than `*If` because the fuzzer mutates an `If` combinator's extra condition
# argument into something that no longer type-checks often enough to lose the regression
# roughly half the time; `*OrNull` takes no extra argument and caught it in 7 of 8 runs.
probe "min_byOrNull" "min_byOrNull(v, v), min_byOrNull(v + 1, v), min_byOrNull(v * 2, v)"

# And one spelling that is neither the registered one nor all-lowercase. `STDDEV_POP` is
# registered as a case-insensitive alias of `stddevPop`, which puts it in the case-sensitive
# alias map under exactly `STDDEV_POP` and in the case-insensitive one under `stddev_pop`, so
# only the second map can resolve `StdDev_Pop`. The backstop set does not contain
# `stddev_pop` either (it names `stddevPop`, and `StdDev_Pop` lowercases to `stddev_pop`,
# not to `stddevpop`), so nothing but the case-insensitive alias lookup can reject this
# query. All the alias probes above use a spelling that resolves through the case-sensitive
# map alone and would stay green if that lookup were dropped.
probe "StdDev_Pop" "StdDev_Pop(v), StdDev_Pop(v + 1), StdDev_Pop(v * 2)"

# Positive controls: an alias whose canonical name is safe must still be checked. Without
# them every probe above would be satisfied just as well by a checker that rejected all
# aliases outright. Retried until the counter moves, because a single mutation can
# occasionally break oracle eligibility for an unrelated reason (see 04658). The retry
# budget is bounded well below the point where the loop alone could exhaust the 180s
# per-test limit: it normally succeeds on the first round, and if it genuinely never fires
# it is far better to report that below than to be killed as a timeout.
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
        after=$(run_fuzzed_rounds "SELECT $aggregates FROM oracle_alias_agg WHERE v > 5;")
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

# `BIT_AND` / `BIT_OR` / `BIT_XOR` are aliases of `groupBitAnd` / `groupBitOr` / `groupBitXor`,
# which are associative and commutative over integers, are therefore absent from the backstop
# set, and must stay checkable through their alias spelling too. The spellings here carry the
# two peculiarities of the last two probes - an `OrNull` combinator on top of the alias, and a
# mixed-case spelling of a case-insensitive alias - so that neither probe can pass vacuously:
# this proves both shapes are checkable in principle, i.e. that the zero delta on
# `min_byOrNull` is the gate rejecting `argMin` and the one on `StdDev_Pop` is it rejecting
# `stddevPop`, not the oracle declining those shapes.
positive_control "safe aliases" "Bit_And(v), BIT_OROrNull(v), BIT_XOR(v)"

# And one alias whose canonical name is a `quantile*`: `medianDeterministic` resolves to
# `quantileDeterministic`, which is NOT on the backstop list because
# `ReservoirSamplerDeterministic` retains a sample purely by hash and re-thins on merge, so a
# merged state equals the directly accumulated one. This is the probe that catches alias
# resolution reaching a neighbouring `quantile*` entry (`median` and the approximate
# `quantile*` families ARE listed) and skipping a checkable query.
# The determinator is the constant `1` rather than `v`: with `v` in that position the oracle
# skips the query for an unrelated reason (the `State`/`Merge` rewrite of a determinator
# that is itself the aggregated column does not survive), which would make the probe
# vacuous. A constant determinator still exercises the alias path, which is what is asserted.
positive_control "medianDeterministic" "medianDeterministic(v, 1)"

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_alias_agg"
