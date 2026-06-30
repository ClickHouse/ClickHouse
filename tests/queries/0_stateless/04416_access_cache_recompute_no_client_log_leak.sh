#!/usr/bin/env bash
# Tags: no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The access-entity caches (QuotaCache, RoleCache, RowPolicyCache, SettingsProfilesCache)
# recompute every enabled set on each access change, in a batch-finished callback that runs on
# whichever client thread issued the DDL, and log the recompute size and duration. Those lines
# are server-internal diagnostics emitted at TRACE (fast recompute) / DEBUG (slow, >= 1000 ms),
# both below the default client level send_logs_level=warning, so at that default level they are
# not forwarded onto the issuing client's stderr (which clickhouse-test treats as a failure).
# Asserts the guaranteed contract, exercising all four caches at two client levels:
#   trace   -> forwarded (1)   the recompute path actually runs and the lines exist
#   warning -> not forwarded (0)   the contract: the default client level no longer leaks them
# debug is intentionally not probed: the slow >= 1000 ms path is LOG_DEBUG by design and so is
# forwarded to an explicit debug client, so a debug assertion would flip whenever a recompute
# crosses the threshold under load -- the exact flakiness this change removes.
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
leaked() { echo "${LEAK_STDERR}" | grep -cE "${PATTERN}"; }

# Create all four entity kinds wired to a user, then drop them, at $1's client log level. Both the
# add and the drop recompute the enabled sets and emit a log line, so each call exercises the path.
# Echoes 1 if any recompute line was forwarded to this client, else 0.
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
    [ "$(leaked)" -gt 0 ] && echo 1 || echo 0
}

drop_all
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${POLICY_TABLE}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${POLICY_TABLE} (x UInt8) ENGINE = Memory"

exercise "${CLIENT_TRACE}"      # 1: recompute lines ARE forwarded at trace (path is exercised)
exercise "${CLIENT_WARNING}"    # 0: the contract -- not forwarded at the default client level

drop_all
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${POLICY_TABLE}"
