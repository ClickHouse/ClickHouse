#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two things this test exercises that the SQL test can't:
#   1. Exact database restoration — the .sql test can only check that we
#      moved away from `system`; here we capture the connection-start
#      database in a shell variable and assert it's restored exactly.
#   2. Settings profile change — `SET profile = 'other'` and verify
#      currentProfiles() reverts after RESET SESSION.

USER="reset_profile_user_${CLICKHOUSE_DATABASE}"
PROF_A="reset_profile_a_${CLICKHOUSE_DATABASE}"
PROF_B="reset_profile_b_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -m -q "
        DROP USER IF EXISTS ${USER};
        DROP SETTINGS PROFILE IF EXISTS ${PROF_A};
        DROP SETTINGS PROFILE IF EXISTS ${PROF_B};
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE SETTINGS PROFILE ${PROF_A} SETTINGS max_threads = 11;
    CREATE SETTINGS PROFILE ${PROF_B} SETTINGS max_threads = 22;
    CREATE USER ${USER} SETTINGS PROFILE ${PROF_A};
    GRANT SELECT, CREATE TEMPORARY TABLE ON *.* TO ${USER};
"

# Capture the database the connection lands in (we don't pass --database so
# this is the user's default, which for the `default`-style user is empty;
# but the client falls back to `default`).
ORIG_DB=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT currentDatabase()")

# Single TCP session that mutates everything we care about and then resets.
${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
    SELECT 'profile at session start:', has(currentProfiles(), '${PROF_A}');
    SELECT 'database at session start matches captured:', currentDatabase() = '${ORIG_DB}';
    SET profile = '${PROF_B}';
    SELECT 'profile after SET:', has(currentProfiles(), '${PROF_B}');
    USE system;
    SELECT 'database after USE:', currentDatabase();
    RESET SESSION;
    SELECT 'profile after RESET:', has(currentProfiles(), '${PROF_A}'), has(currentProfiles(), '${PROF_B}');
    SELECT 'database after RESET matches start:', currentDatabase() = '${ORIG_DB}';
" | sed "s/_${CLICKHOUSE_DATABASE}/_DB/g"
