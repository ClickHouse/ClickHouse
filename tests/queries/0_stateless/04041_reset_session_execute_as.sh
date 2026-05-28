#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `RESET SESSION` must return the session to the *authenticated* user — not
# whichever user the session is currently impersonating via `EXECUTE AS`.
# Before the fix, `Context::resetToUserDefaults` re-derived defaults from the
# current `user_id`, which `EXECUTE AS` mutates on the session context. A
# pooled connection that ran `EXECUTE AS target` and was then handed back
# after `RESET SESSION` would silently stay impersonating `target` for the
# next borrower.
#
# This test pins the contract end-to-end in a single TCP session:
# `EXECUTE AS impersonated; RESET SESSION;` lands back as the authenticated
# user. We use a unique user per `${CLICKHOUSE_DATABASE}` because EXECUTE AS
# requires `GRANT IMPERSONATE`, which is mediated through access control.

AUTH_USER="reset_exec_auth_${CLICKHOUSE_DATABASE}"
IMP_USER="reset_exec_imp_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -m -q "
        DROP USER IF EXISTS ${AUTH_USER};
        DROP USER IF EXISTS ${IMP_USER};
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE USER ${AUTH_USER};
    CREATE USER ${IMP_USER};
    GRANT IMPERSONATE ON ${IMP_USER} TO ${AUTH_USER};
"

# Run all assertions in a single `clickhouse-client` invocation so they share
# one TCP session — `EXECUTE AS` without a subquery mutates the session
# context, and we need `RESET SESSION` to see that mutated state to prove the
# baseline-restore.
${CLICKHOUSE_CLIENT} --user "${AUTH_USER}" -m -q "
    SELECT '-- pre-impersonation --';
    SELECT currentUser() = '${AUTH_USER}' AS current_is_auth,
           authenticatedUser() = '${AUTH_USER}' AS authd_is_auth;

    EXECUTE AS ${IMP_USER};

    SELECT '-- during impersonation --';
    SELECT currentUser() = '${IMP_USER}' AS current_is_imp,
           authenticatedUser() = '${AUTH_USER}' AS authd_still_auth;

    RESET SESSION;

    SELECT '-- post-reset --';
    SELECT currentUser() = '${AUTH_USER}' AS current_back_to_auth,
           authenticatedUser() = '${AUTH_USER}' AS authd_back_to_auth;
"
