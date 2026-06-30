#!/usr/bin/env bash
# Tags: no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The access-entity caches (QuotaCache, RoleCache, RowPolicyCache, SettingsProfilesCache)
# recompute every enabled set on each access change, in a batch-finished callback that runs on
# whichever client thread issued the DDL, and log the recompute size and duration. Those lines
# are server-internal diagnostics and are emitted at DEBUG (slow recompute) / TRACE (fast), below
# the default client level send_logs_level=warning, so they are not forwarded onto the issuing
# client's stderr (which clickhouse-test would treat as a failure). This pins the fast-path line
# at TRACE: forwarded at send_logs_level=trace, not at =debug.
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

drop_all
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${POLICY_TABLE}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${POLICY_TABLE} (x UInt8) ENGINE = Memory"

# CREATE batch at send_logs_level=trace: the recompute lines ARE forwarded. Asserting they appear
# proves the path ran and the lines exist, so the debug-level 0 below is meaningful.
run_ddl "${CLIENT_TRACE}" "
    CREATE ROLE ${ROLE};
    CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1 TO ${ROLE};
    CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'pass';
    GRANT ${ROLE} TO ${USER};
    CREATE ROW POLICY ${POLICY} ON ${POLICY_TABLE} USING 1 TO ${USER};
    CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${USER};
"
[ "$(leaked)" -gt 0 ] && echo 1 || echo 0

# DROP batch at send_logs_level=debug: the same caches recompute, but the lines are TRACE and must
# NOT be forwarded at debug. This is the regression guard (0 leaked lines).
run_ddl "${CLIENT_DEBUG}" "
    DROP USER ${USER};
    DROP QUOTA ${QUOTA};
    DROP ROW POLICY ${POLICY} ON ${POLICY_TABLE};
    DROP ROLE ${ROLE};
    DROP SETTINGS PROFILE ${PROFILE};
"
[ "$(leaked)" -gt 0 ] && echo 1 || echo 0

drop_all
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${POLICY_TABLE}"
