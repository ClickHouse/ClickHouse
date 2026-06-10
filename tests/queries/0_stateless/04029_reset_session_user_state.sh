#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="reset_session_user_${CLICKHOUSE_DATABASE}"
ROLE_A="reset_session_role_a_${CLICKHOUSE_DATABASE}"
ROLE_B="reset_session_role_b_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -m -q "
        DROP USER IF EXISTS ${USER};
        DROP ROLE IF EXISTS ${ROLE_A};
        DROP ROLE IF EXISTS ${ROLE_B};
    "
}
trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE ROLE ${ROLE_A};
    CREATE ROLE ${ROLE_B};
    CREATE USER ${USER} DEFAULT ROLE ${ROLE_A};
    GRANT ${ROLE_A}, ${ROLE_B} TO ${USER};
    GRANT SELECT ON *.* TO ${ROLE_A};
    GRANT SELECT ON *.* TO ${ROLE_B};
"

# Run a multi-statement script in a single TCP session, switching the active
# role mid-session and verifying that RESET SESSION restores the default.
# The role names are randomised per test database; replace the suffix with a
# stable placeholder so the reference file is deterministic.
${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
    SELECT 'roles at session start:', arraySort(currentRoles());
    SET ROLE ${ROLE_B};
    SELECT 'roles after SET ROLE:', arraySort(currentRoles());
    RESET SESSION;
    SELECT 'roles after RESET SESSION:', arraySort(currentRoles());
    SET ROLE NONE;
    SELECT 'roles after SET ROLE NONE:', arraySort(currentRoles());
    RESET SESSION;
    SELECT 'roles after second RESET SESSION:', arraySort(currentRoles());
" | sed "s/_${CLICKHOUSE_DATABASE}/_DB/g"
