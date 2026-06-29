#!/usr/bin/env bash
# Tags: no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The access-entity caches (QuotaCache, RoleCache, RowPolicyCache, SettingsProfilesCache)
# recompute every enabled set on each access change. That recompute runs in the batch-finished
# callback on whichever client thread issued the DDL, and it logs its size and duration. Those
# server-internal maintenance logs must NOT be forwarded to the client: otherwise a client with
# send_logs_level<=debug (or <=warning, when a slow recompute escalates the line to WARNING)
# receives them on stderr, which clickhouse-test treats as a failure ("having stderror").
#
# Force a connection-level send_logs_level=debug: the recompute fires after the DDL statement's
# per-query settings are gone, so a query-level SETTINGS override would not apply -- only the
# connection-level log level governs it. Replace the runner-injected level in CLICKHOUSE_CLIENT
# rather than appending (clickhouse-client rejects a repeated --send_logs_level).
if echo "${CLICKHOUSE_CLIENT}" | grep -q -- "--send_logs_level"; then
    CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed -E 's/--send_logs_level=[a-zA-Z]+/--send_logs_level=debug/')
else
    CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=debug"
fi

USER="user_${CLICKHOUSE_DATABASE}"
QUOTA="quota_${CLICKHOUSE_DATABASE}"
ROLE="role_${CLICKHOUSE_DATABASE}"
POLICY="policy_${CLICKHOUSE_DATABASE}"
PROFILE="profile_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${POLICY} ON system.one"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"

# Each statement triggers a batch-finished recompute of one or more caches. We request debug
# logs at the connection level so any leaked recompute line would deterministically appear on
# stderr. The recompute lines must stay in the server log only; capture stderr and assert none
# of them leaked.
stderr=$(
    ${CLIENT} --multiquery -q "
        CREATE ROLE ${ROLE};
        CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1 TO ${ROLE};
        CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'pass';
        GRANT ${ROLE} TO ${USER};
        CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${USER};
        CREATE ROW POLICY ${POLICY} ON system.one USING 1 TO ${USER};
        ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 4 TO ${USER};
    " 2>&1 1>/dev/null
)

echo "$stderr" | grep -cE "QuotaCache: Re-chose quotas|RoleCache: Recalculated enabled roles|RowPolicyCache: Re-mixed row policy|SettingsProfilesCache: Re-merged settings and constraints"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${POLICY} ON system.one"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
