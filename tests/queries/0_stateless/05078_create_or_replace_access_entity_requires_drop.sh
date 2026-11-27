#!/usr/bin/env bash
# `CREATE ... OR REPLACE` overwrites an existing access entity, so it must require the corresponding
# `DROP` privilege on top of `CREATE`. Otherwise `CREATE USER` alone would be enough to reset the
# password of an arbitrary, possibly privileged, user and then log in as that user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

suffix="05078_${CLICKHOUSE_DATABASE}"
actor="actor_${suffix}"
victim_user="victim_user_${suffix}"
victim_role="victim_role_${suffix}"
victim_quota="victim_quota_${suffix}"
victim_profile="victim_profile_${suffix}"
victim_policy="victim_policy_${suffix}"
new_user="new_user_${suffix}"
new_role="new_role_${suffix}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP ROW POLICY IF EXISTS ${victim_policy} ON ${CLICKHOUSE_DATABASE}.target;
        DROP USER IF EXISTS ${actor}, ${victim_user}, ${new_user};
        DROP ROLE IF EXISTS ${victim_role}, ${new_role};
        DROP QUOTA IF EXISTS ${victim_quota};
        DROP SETTINGS PROFILE IF EXISTS ${victim_profile};
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.target;
    "
}

trap cleanup EXIT
cleanup

# Prints `denied` when the statement was rejected for the expected missing privilege and `allowed` when it
# went through. Anything else is printed verbatim, so an unrelated failure is not read as a denial.
check()
{
    local expected_privilege="$1"
    local query="$2"
    local output
    if output="$($CLICKHOUSE_CLIENT --user "${actor}" --query "$query" 2>&1)"
    then
        echo "allowed"
    elif grep -q "Not enough privileges" <<<"$output" && grep -q "${expected_privilege}" <<<"$output"
    then
        echo "denied"
    else
        echo "$output"
    fi
}

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.target (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE USER ${actor};
    CREATE USER ${victim_user} IDENTIFIED WITH plaintext_password BY 'victim_password';
    CREATE ROLE ${victim_role};
    CREATE QUOTA ${victim_quota};
    CREATE SETTINGS PROFILE ${victim_profile};
    CREATE ROW POLICY ${victim_policy} ON ${CLICKHOUSE_DATABASE}.target USING id = 1 TO ${actor};
    GRANT CREATE USER, CREATE ROLE, CREATE QUOTA, CREATE SETTINGS PROFILE ON *.* TO ${actor};
    GRANT CREATE ROW POLICY ON ${CLICKHOUSE_DATABASE}.* TO ${actor};
"

echo "-- only CREATE granted"
check "DROP USER" "CREATE USER OR REPLACE ${victim_user} IDENTIFIED WITH plaintext_password BY 'hijacked'"
check "DROP ROLE" "CREATE ROLE OR REPLACE ${victim_role}"
check "DROP QUOTA" "CREATE QUOTA OR REPLACE ${victim_quota}"
check "DROP SETTINGS PROFILE" "CREATE SETTINGS PROFILE OR REPLACE ${victim_profile}"
check "DROP ROW POLICY" "CREATE ROW POLICY OR REPLACE ${victim_policy} ON ${CLICKHOUSE_DATABASE}.target USING id = 2 TO ${actor}"

echo "-- the victim user kept its original password"
$CLICKHOUSE_CLIENT --user "${victim_user}" --password 'victim_password' --query "SELECT 'login ok'"

echo "-- the requirement does not depend on the entity already existing"
check "DROP USER" "CREATE USER OR REPLACE ${new_user}"
check "DROP ROLE" "CREATE ROLE OR REPLACE ${new_role}"

echo "-- plain CREATE is unaffected"
check "DROP USER" "CREATE USER ${new_user}"
check "DROP ROLE" "CREATE ROLE ${new_role}"

$CLICKHOUSE_CLIENT --query "
    GRANT DROP USER, DROP ROLE, DROP QUOTA, DROP SETTINGS PROFILE ON *.* TO ${actor};
    GRANT DROP ROW POLICY ON ${CLICKHOUSE_DATABASE}.* TO ${actor};
"

echo "-- DROP granted as well"
check "DROP USER" "CREATE USER OR REPLACE ${victim_user} IDENTIFIED WITH plaintext_password BY 'replaced'"
check "DROP ROLE" "CREATE ROLE OR REPLACE ${victim_role}"
check "DROP QUOTA" "CREATE QUOTA OR REPLACE ${victim_quota}"
check "DROP SETTINGS PROFILE" "CREATE SETTINGS PROFILE OR REPLACE ${victim_profile}"
check "DROP ROW POLICY" "CREATE ROW POLICY OR REPLACE ${victim_policy} ON ${CLICKHOUSE_DATABASE}.target USING id = 2 TO ${actor}"

echo "-- the victim user now has the new password"
$CLICKHOUSE_CLIENT --user "${victim_user}" --password 'replaced' --query "SELECT 'login ok'"
