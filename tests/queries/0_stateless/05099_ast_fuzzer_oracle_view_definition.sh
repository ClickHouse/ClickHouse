#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: required because the proof events are server-global.
# long:        the flaky check repeats a test many times and fails any single run whose wall
#              clock exceeds `TEST_MAX_RUN_TIME_IN_SECONDS`; the retry loops below cannot fit
#              that budget. The tag waives that limit and lowers the repeat count. Accepted
#              cost: a `--no-long` job skips this test.
#
# A query names relations and columns; reading them evaluates the definitions stored
# behind those names, and a read re-evaluates them, so a non-deterministic function
# hidden behind any one of them makes each of the oracle's reads observe a different
# value and the oracle reports a mismatch that is not a wrong result.
# `QueryOracleChecker::check` must therefore screen those definitions, not only the
# query text.
#
# The assertion is on `ASTFuzzerOracleChecks`, not on "the query succeeded": with
# `ast_fuzzer_runs = 1` a random mutation can make a query oracle-ineligible for
# unrelated reasons, so an absent mismatch alone cannot distinguish "screened" from
# "never checked". A mismatch increments `ASTFuzzerOracleChecks` before comparing, so an
# unscreened definition moves the counter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_definition_src;
    DROP TABLE IF EXISTS oracle_definition_alias;
    DROP TABLE IF EXISTS oracle_definition_det_alias;
    DROP TABLE IF EXISTS oracle_definition_default;
    DROP TABLE IF EXISTS oracle_definition_det_default;
    DROP TABLE IF EXISTS oracle_definition_materialized;
    DROP TABLE IF EXISTS oracle_definition_ephemeral;
    DROP TABLE IF EXISTS oracle_definition_alias_engine;
    DROP TABLE IF EXISTS oracle_definition_row_policy;
    DROP TABLE IF EXISTS oracle_definition_det_row_policy;
    DROP TABLE IF EXISTS oracle_definition_definer_src;
    DROP VIEW IF EXISTS oracle_definition_definer_view;
    DROP ROW POLICY IF EXISTS oracle_definition_nondet_filter ON oracle_definition_row_policy;
    DROP ROW POLICY IF EXISTS oracle_definition_det_filter ON oracle_definition_det_row_policy;
    DROP ROW POLICY IF EXISTS oracle_definition_definer_filter ON oracle_definition_definer_src;
    DROP USER IF EXISTS oracle_definition_definer_${CLICKHOUSE_DATABASE};
    DROP VIEW IF EXISTS oracle_definition_mv;
    DROP VIEW IF EXISTS oracle_definition_nondet_view;
    DROP VIEW IF EXISTS oracle_definition_det_view;
    DROP VIEW IF EXISTS oracle_definition_inner_view;
    DROP VIEW IF EXISTS oracle_definition_outer_view;
    DROP VIEW IF EXISTS oracle_definition_system_view;
    DROP VIEW IF EXISTS oracle_definition_in_view;
    DROP VIEW IF EXISTS oracle_definition_in_det_view;

    CREATE TABLE oracle_definition_src (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_src SELECT number FROM numbers(50);

    -- The view body holds the non-determinism; the query below names only the view.
    CREATE VIEW oracle_definition_nondet_view AS SELECT k, rand() AS r FROM oracle_definition_src;

    -- Same shape, deterministic body: the anti-vacuity control below.
    CREATE VIEW oracle_definition_det_view AS SELECT k, k * 2 AS r FROM oracle_definition_src;

    -- Two levels of naming: only the inner body is non-deterministic, so reaching it
    -- requires recursing through the outer definition.
    CREATE VIEW oracle_definition_inner_view AS SELECT k, rand() AS r FROM oracle_definition_src;
    CREATE VIEW oracle_definition_outer_view AS SELECT k, r FROM oracle_definition_inner_view;

    -- The \`system\` reference is in the body, so only the definition screen can see it.
    CREATE VIEW oracle_definition_system_view AS SELECT event, value FROM system.events;

    -- Named as an \`IN\` operand, which is a plain identifier until analysis rewrites it.
    -- \`k IN v\` requires a single-column \`v\`, so the two-column views above cannot be reused.
    CREATE VIEW oracle_definition_in_view AS SELECT rand() % 50 AS k FROM oracle_definition_src;
    CREATE VIEW oracle_definition_in_det_view AS SELECT k FROM oracle_definition_src;

    -- The column expression holds the non-determinism; the query names only the column.
    CREATE TABLE oracle_definition_alias (k UInt32, r UInt32 ALIAS rand()) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_alias SELECT number FROM numbers(50);

    -- Same shape, deterministic expression: the second anti-vacuity control.
    CREATE TABLE oracle_definition_det_alias (k UInt32, r UInt32 ALIAS k * 2) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_det_alias SELECT number FROM numbers(50);

    -- A column added after a part was written is not stored in it, so a read of that column
    -- evaluates its \`DEFAULT\` expression, which is therefore a read-time definition too.
    -- Two reads of this unchanged part return different values for \`r\`.
    CREATE TABLE oracle_definition_default (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_default SELECT number FROM numbers(50);
    ALTER TABLE oracle_definition_default ADD COLUMN r UInt32 DEFAULT rand();

    -- Same shape, deterministic expression: separates \"screen a non-deterministic default\"
    -- from \"screen every column that has a default\".
    CREATE TABLE oracle_definition_det_default (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_det_default SELECT number FROM numbers(50);
    ALTER TABLE oracle_definition_det_default ADD COLUMN r UInt32 DEFAULT k * 2;

    -- \`MATERIALIZED\` is stored, so it reaches the same read-time path only for a part
    -- written before the column existed.
    CREATE TABLE oracle_definition_materialized (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_materialized SELECT number FROM numbers(50);
    ALTER TABLE oracle_definition_materialized ADD COLUMN r UInt32 MATERIALIZED rand();

    -- An \`EPHEMERAL\` column cannot be read directly, but supplying a missing column pulls in
    -- the defaults of the columns its own expression needs, so \`rand()\` here is evaluated by
    -- a read of \`r\` and the ephemeral expression is reachable after all.
    CREATE TABLE oracle_definition_ephemeral (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_ephemeral SELECT number FROM numbers(50);
    ALTER TABLE oracle_definition_ephemeral ADD COLUMN e UInt32 EPHEMERAL rand();
    ALTER TABLE oracle_definition_ephemeral ADD COLUMN r UInt32 DEFAULT e;

    -- A row policy for the current user filters every read of the table it is attached to, and
    -- its filter lives on the policy rather than in the table's metadata, so two reads of these
    -- unchanged 50 rows return different subsets of them.
    CREATE TABLE oracle_definition_row_policy (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_row_policy SELECT number FROM numbers(50);
    CREATE ROW POLICY oracle_definition_nondet_filter ON oracle_definition_row_policy
        USING (rand() % 2) = 0 TO ALL;

    -- Same shape, deterministic filter: separates \"screen a non-deterministic policy\" from
    -- \"reject every table that has a policy\".
    CREATE TABLE oracle_definition_det_row_policy (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_det_row_policy SELECT number FROM numbers(50);
    CREATE ROW POLICY oracle_definition_det_filter ON oracle_definition_det_row_policy
        USING k < 1000000 TO ALL;

    -- A \`DEFINER\` view evaluates its body as the definer, so the policy a read of it applies is
    -- the definer's: this body names only \`k\`, the reader below has no policy on the base table,
    -- and two reads of it still return different subsets.
    CREATE TABLE oracle_definition_definer_src (k UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO oracle_definition_definer_src SELECT number FROM numbers(50);
    CREATE USER oracle_definition_definer_${CLICKHOUSE_DATABASE} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.oracle_definition_definer_src
        TO oracle_definition_definer_${CLICKHOUSE_DATABASE};
    CREATE ROW POLICY oracle_definition_definer_filter ON oracle_definition_definer_src
        USING (rand() % 2) = 0 TO oracle_definition_definer_${CLICKHOUSE_DATABASE};
    CREATE VIEW oracle_definition_definer_view
        DEFINER = oracle_definition_definer_${CLICKHOUSE_DATABASE} SQL SECURITY DEFINER
        AS SELECT k FROM oracle_definition_definer_src;

    -- \`Alias\` reports its own engine name while reading, and reporting the metadata of,
    -- its target, so the target view's body is reachable but not named here.
    SET allow_experimental_alias_table_engine = 1;
    CREATE TABLE oracle_definition_alias_engine ENGINE = Alias(currentDatabase(), 'oracle_definition_nondet_view');

    -- A materialized view read is forwarded to its target table, whose engine and column
    -- defaults are in the target's metadata rather than in the view's, so no screen of this
    -- name can describe what the read evaluates. Its own body is not read, hence not screened.
    CREATE MATERIALIZED VIEW oracle_definition_mv ENGINE = MergeTree ORDER BY k
        POPULATE AS SELECT k FROM oracle_definition_src;
"

get_checks()
{
    $CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events WHERE event = 'ASTFuzzerOracleChecks'"
}

# Runs one fuzzed query and echoes `<counter>TAB<logged query>`, so a caller can tell both how
# many oracles had run and what the oracle saw from a single client invocation.
#
# The counter is read FIRST, before the fuzzer is enabled, which makes it the value that closes
# the *previous* iteration rather than this one. That ordering is required, not stylistic: a
# multi-statement `--query` stops at the first failing statement, and an oracle mismatch is
# rethrown to the client, so a counter read placed after the query is lost in exactly the runs
# where an oracle ran and disagreed.
#
# The query's own rows share the stream with the trace line and are dropped by the filter rather
# than by `FORMAT Null`, because the oracle skips queries carrying an explicit FORMAT clause.
# That also hides the expected error-level lines from mutations producing valid-but-nonsense
# queries. A fuzzed query can contain arbitrary bytes, and the runner fails a test that writes
# anything to stderr, so NUL bytes are stripped before the match and `grep -a` keeps it from
# reporting the stream as binary instead of matching.
# `$2` runs before the fuzzer is enabled and in the same session as the query, which is
# what a session-bound object needs in order to exist, unfuzzed, when the query reads it.
run_fuzzed()
{
    local out
    out=$($CLICKHOUSE_CLIENT --query "
        SELECT 'ORACLE_CHECKS=' || toString(sum(value)) FROM system.events WHERE event = 'ASTFuzzerOracleChecks';
        ${2:-}
        SET send_logs_level = 'trace';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        $1
    " 2>&1 | tr -d '\0' | grep -a -o -E 'ORACLE_CHECKS=[0-9]+|Fuzzed query: .*')

    printf '%s\t%s\n' \
        "$(printf '%s\n' "$out" | grep -a -o -E 'ORACLE_CHECKS=[0-9]+' | tail -1 | cut -d '=' -f 2)" \
        "$(printf '%s\n' "$out" | grep -a 'Fuzzed query: ' | tail -1)"
}

# A screened definition must never reach an oracle, so the counter must not move for a fuzzed
# query that still names `$2`. A mutation may drop that reference instead of preserving it (the
# fuzzer rewrites a WHERE predicate freely, so `k IN v` can become `k`), and an oracle running
# on a query that no longer reads the definition is correct rather than a leak, so each move is
# attributed to the query the server logged. A move that cannot be attributed counts as a leak.
#
# Each sample carries the counter as it stood before its own fuzzed query, so a move is visible
# one sample after the query that caused it; the trailing read closes the last iteration.
assert_screened()
{
    local label=$1
    local object=$2
    local query=$3
    local prelude=${4:-}
    local runs=10
    local sample checks fuzzed prev_checks="" prev_fuzzed="" positions
    local leaked=0
    local i

    for i in $(seq 1 $((runs + 1)))
    do
        if [[ "$i" -le "$runs" ]]
        then
            sample=$(run_fuzzed "$query" "$prelude")
            checks=${sample%%$'\t'*}
            fuzzed=${sample#*$'\t'}
        else
            checks=$(get_checks)
            fuzzed=""
        fi

        # No counter at all means the invocation never reached its first statement, so this
        # sample pairs with nothing: drop the pairing instead of attributing the gap to a query.
        if [[ -z "$checks" ]]
        then
            prev_checks=""
            prev_fuzzed=""
            continue
        fi

        if [[ -n "$prev_checks" && "$checks" -gt "$prev_checks" ]]
        then
            # A parameterized identifier logs as `{fuzz_param_N:Identifier}`, so a move paired with
            # one is unattributable by name; in a table position it is a leak whatever the
            # placeholder stands for, because the screen rejects that name before any oracle runs.
            # `ARRAY JOIN` / `IS DISTINCT FROM` end in those keywords but take a column: blind them.
            positions=${prev_fuzzed//ARRAY JOIN/ARRAY-J}
            positions=${positions//IS DISTINCT FROM/IS-DF}
            if [[ -z "$prev_fuzzed" || "$prev_fuzzed" == *"$object"* \
                  || "$positions" =~ (FROM|JOIN)[[:space:]]*\{[A-Za-z0-9_]*:Identifier\} \
                  || "$positions" =~ IN[[:space:]]*\([[:space:]]*\{[A-Za-z0-9_]*:Identifier\}\) ]]
            then
                leaked=1
                break
            fi
        fi

        prev_checks=$checks
        prev_fuzzed=$fuzzed
    done

    if [[ "$leaked" -eq 0 ]]
    then
        echo "$label: oracle skipped"
    else
        echo "$label: oracle ran on an unscreened definition"
    fi
}

# Anti-vacuity: a screen that rejected every named definition wholesale would pass every
# assertion above. One eligible pass that still names `$2` must reach an oracle. Attributed for
# the same reason as above, and here it is what keeps the control sharp: a mutation that dropped
# the reference is eligible whatever the screen does, so counting it would let a screen that
# rejects the construct outright pass this assertion. Retried because a single mutation can break
# oracle eligibility for reasons unrelated to the definition screen.
assert_reaches_oracle()
{
    local label=$1
    local object=$2
    local query=$3
    local sample checks fuzzed prev_checks="" prev_fuzzed=""
    local ran=0
    local i

    for i in $(seq 1 100)
    do
        sample=$(run_fuzzed "$query")
        checks=${sample%%$'\t'*}
        fuzzed=${sample#*$'\t'}

        if [[ -z "$checks" ]]
        then
            prev_checks=""
            prev_fuzzed=""
            continue
        fi

        if [[ -n "$prev_checks" && "$checks" -gt "$prev_checks" && "$prev_fuzzed" == *"$object"* ]]
        then
            ran=1
            break
        fi

        prev_checks=$checks
        prev_fuzzed=$fuzzed
    done

    if [[ "$ran" -eq 1 ]]
    then
        echo "$label: oracle ran"
    else
        echo "$label: oracle never ran on a query naming $object"
    fi
}

assert_screened "view over a non-deterministic definition" oracle_definition_nondet_view \
    "SELECT k, r FROM oracle_definition_nondet_view WHERE k > 5;"

assert_reaches_oracle "view over a deterministic definition" oracle_definition_det_view \
    "SELECT k, r FROM oracle_definition_det_view WHERE k > 5;"

assert_screened "view over a view over a non-deterministic definition" oracle_definition_outer_view \
    "SELECT k, r FROM oracle_definition_outer_view WHERE k > 5;"

assert_screened "view over a system table" oracle_definition_system_view \
    "SELECT event, value FROM oracle_definition_system_view WHERE value > 0;"

# A temporary view lives in the session rather than in a database, so it is created in the
# same client invocation as the query that reads it.
assert_screened "temporary view over a non-deterministic definition" oracle_definition_temp_view \
    "SELECT k, r FROM oracle_definition_temp_view WHERE k > 5;" \
    "CREATE TEMPORARY VIEW oracle_definition_temp_view AS SELECT k, rand() AS r FROM oracle_definition_src;"

assert_screened "Alias engine over a non-deterministic definition" oracle_definition_alias_engine \
    "SELECT k, r FROM oracle_definition_alias_engine WHERE k > 5;"

assert_screened "materialized view" oracle_definition_mv \
    "SELECT k FROM oracle_definition_mv WHERE k > 5;"

assert_screened "ALIAS column with a non-deterministic expression" oracle_definition_alias \
    "SELECT k, r FROM oracle_definition_alias WHERE k > 5;"

assert_reaches_oracle "ALIAS column with a deterministic expression" oracle_definition_det_alias \
    "SELECT k, r FROM oracle_definition_det_alias WHERE k > 5;"

assert_screened "DEFAULT column with a non-deterministic expression" oracle_definition_default \
    "SELECT k, r FROM oracle_definition_default WHERE k > 5;"

assert_reaches_oracle "DEFAULT column with a deterministic expression" oracle_definition_det_default \
    "SELECT k, r FROM oracle_definition_det_default WHERE k > 5;"

assert_screened "MATERIALIZED column with a non-deterministic expression" oracle_definition_materialized \
    "SELECT k, r FROM oracle_definition_materialized WHERE k > 5;"

assert_screened "EPHEMERAL expression reached through a DEFAULT" oracle_definition_ephemeral \
    "SELECT k, r FROM oracle_definition_ephemeral WHERE k > 5;"

assert_screened "IN over a non-deterministic definition" oracle_definition_in_view \
    "SELECT k FROM oracle_definition_src WHERE k IN oracle_definition_in_view;"

assert_reaches_oracle "IN over a deterministic definition" oracle_definition_in_det_view \
    "SELECT k FROM oracle_definition_src WHERE k IN oracle_definition_in_det_view;"

assert_screened "row policy with a non-deterministic filter" oracle_definition_row_policy \
    "SELECT k FROM oracle_definition_row_policy WHERE k > 5;"

assert_reaches_oracle "row policy with a deterministic filter" oracle_definition_det_row_policy \
    "SELECT k FROM oracle_definition_det_row_policy WHERE k > 5;"

assert_screened "DEFINER view over the definer's row policy" oracle_definition_definer_view \
    "SELECT k FROM oracle_definition_definer_view WHERE k > 5;"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW oracle_definition_definer_view;
    DROP ROW POLICY oracle_definition_definer_filter ON oracle_definition_definer_src;
    DROP USER oracle_definition_definer_${CLICKHOUSE_DATABASE};
    DROP TABLE oracle_definition_definer_src;
    DROP ROW POLICY oracle_definition_det_filter ON oracle_definition_det_row_policy;
    DROP ROW POLICY oracle_definition_nondet_filter ON oracle_definition_row_policy;
    DROP TABLE oracle_definition_det_row_policy;
    DROP TABLE oracle_definition_row_policy;
    DROP VIEW oracle_definition_mv;
    DROP TABLE oracle_definition_alias_engine;
    DROP TABLE oracle_definition_ephemeral;
    DROP TABLE oracle_definition_materialized;
    DROP TABLE oracle_definition_det_default;
    DROP TABLE oracle_definition_default;
    DROP VIEW oracle_definition_in_det_view;
    DROP VIEW oracle_definition_in_view;
    DROP VIEW oracle_definition_system_view;
    DROP VIEW oracle_definition_outer_view;
    DROP VIEW oracle_definition_inner_view;
    DROP VIEW oracle_definition_nondet_view;
    DROP VIEW oracle_definition_det_view;
    DROP TABLE oracle_definition_det_alias;
    DROP TABLE oracle_definition_alias;
    DROP TABLE oracle_definition_src;
"
