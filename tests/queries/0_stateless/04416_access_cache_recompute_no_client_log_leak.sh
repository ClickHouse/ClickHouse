#!/usr/bin/env bash
# Tags: no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The access-entity caches (QuotaCache, RoleCache, RowPolicyCache, SettingsProfilesCache)
# recompute every enabled set on each access change, in a batch-finished callback that runs on
# whichever client thread issued the DDL, and log the recompute size and duration. On the fixed
# server those lines are server-internal diagnostics emitted at TRACE (fast recompute) / DEBUG
# (slow, >= 1000 ms); on master (before this change) they are DEBUG (fast) / WARNING (slow), so a
# slow recompute leaks onto the issuing client's stderr at the default send_logs_level=warning
# (which clickhouse-test treats as a failure). Exercises all four caches at three client levels:
#   trace   -> forwarded (1)   the recompute path actually runs and the lines exist
#   debug   -> a fast (< 1000 ms) recompute line NOT forwarded (0)   the deterministic fix check
#   warning -> not forwarded (0)   the user-facing contract: the default level no longer leaks
#
# The debug probe is what makes this a valid regression test (it fails on master, passes on the
# fix), while staying free of the threshold flakiness that a naive debug assertion would have:
#   * On master the fast path is LOG_DEBUG, so a debug client always receives those fast lines
#     (duration < 1000 ms) -> the probe counts >= 1 and the test FAILS on master HEAD.
#   * On the fixed server the fast path is LOG_TRACE (not forwarded to a debug client) and only
#     the slow >= 1000 ms path is LOG_DEBUG. By counting only lines whose measured duration is
#     < 1000 ms we ignore that slow path, so the probe is a deterministic 0 on the fix regardless
#     of how slow a recompute gets under sanitizer/thread-fuzzer load.
#
# send_logs_level is connection-level (the recompute fires after the statement's per-query
# settings are gone), so build a client per level by replacing the runner-injected value
# (clickhouse-client rejects a repeated --send_logs_level).
make_client() {
    if echo "${CLICKHOUSE_CLIENT}" | grep -q -- "--send_logs_level"; then
        echo "${CLICKHOUSE_CLIENT}" | sed -E "s/--send_logs_level=[a-zA-Z]+/--send_logs_level=$1/"
    else
        echo "${CLICKHOUSE_CLIENT} --send_logs_level=$1"
    fi
}
CLIENT_TRACE=$(make_client trace)
CLIENT_DEBUG=$(make_client debug)
CLIENT_WARNING=$(make_client warning)

USER="user_${CLICKHOUSE_DATABASE}"
QUOTA="quota_${CLICKHOUSE_DATABASE}"
ROLE="role_${CLICKHOUSE_DATABASE}"
POLICY="policy_${CLICKHOUSE_DATABASE}"
PROFILE="profile_${CLICKHOUSE_DATABASE}"
# Put the row policy on this test's own private table, never the shared system.one: with
# throw_on_unmatched_row_policies=on a policy on a shared table makes every other session's reads
# of it fail with ACCESS_DENIED. The recompute (and its log line) fires for any target table.
POLICY_TABLE="${CLICKHOUSE_DATABASE}.policy_target"

PATTERN="QuotaCache: Re-chose quotas|RoleCache: Recalculated enabled roles|RowPolicyCache: Re-mixed row policy|SettingsProfilesCache: Re-merged settings and constraints"

drop_all() {
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
    ${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${POLICY} ON ${POLICY_TABLE}"
    ${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
}

# Run a DDL batch at $1's log level, leaving its captured client stderr in LEAK_STDERR. Called at
# top level (never in $(...)): on a non-zero client status it exits the whole test, so an errored
# DDL cannot false-pass with empty stderr that counts as 0 leaked lines without exercising the path.
LEAK_STDERR=""
run_ddl() {
    if ! LEAK_STDERR=$(${1} --multiquery -q "${2}" 2>&1 1>/dev/null); then
        echo "${LEAK_STDERR}"
        exit 1
    fi
}
# Count all recompute lines forwarded to this client.
leaked() { echo "${LEAK_STDERR}" | grep -cE "${PATTERN}"; }
# Count only the fast-path (< 1000 ms) recompute lines. Each line ends with "... in <N> ms"; keep
# the ones with N < 1000 and drop the slow >= 1000 ms lines, which on the fix are the only ones a
# debug client can receive. See the header comment for why this makes the debug probe deterministic.
leaked_fast() {
    echo "${LEAK_STDERR}" \
        | grep -oE "(${PATTERN}).* in [0-9]+ ms" \
        | sed -E 's/.* in ([0-9]+) ms/\1/' \
        | awk '$1 < 1000' \
        | wc -l
}

# Create all four entity kinds wired to a user, then drop them, at $1's client log level. Both the
# add and the drop recompute the enabled sets and emit a log line, so each call exercises the path.
# Counts the forwarded lines with $2 (leaked / leaked_fast) and echoes 1 if any, else 0.
exercise() {
    run_ddl "${1}" "
        CREATE ROLE ${ROLE};
        CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1 TO ${ROLE};
        CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'pass';
        GRANT ${ROLE} TO ${USER};
        CREATE ROW POLICY ${POLICY} ON ${POLICY_TABLE} USING 1 TO ${USER};
        CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${USER};
        DROP QUOTA ${QUOTA};
        DROP ROW POLICY ${POLICY} ON ${POLICY_TABLE};
        DROP USER ${USER};
        DROP ROLE ${ROLE};
        DROP SETTINGS PROFILE ${PROFILE};
    "
    [ "$(${2})" -gt 0 ] && echo 1 || echo 0
}

drop_all
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${POLICY_TABLE}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${POLICY_TABLE} (x UInt8) ENGINE = Memory"

exercise "${CLIENT_TRACE}"   leaked        # 1: recompute lines ARE forwarded at trace (path is exercised)
exercise "${CLIENT_DEBUG}"   leaked_fast   # 0 on the fix (fast path is TRACE); 1 on master (fast path is DEBUG)
exercise "${CLIENT_WARNING}" leaked        # 0: the contract -- not forwarded at the default client level

drop_all
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${POLICY_TABLE}"
