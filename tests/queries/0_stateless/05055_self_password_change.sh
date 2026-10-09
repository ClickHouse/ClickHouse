#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -euo pipefail

prefix="self_password_05055_${CLICKHOUSE_DATABASE}_$RANDOM"
alice="${prefix}_alice"
bob="${prefix}_bob"
replay="${prefix}_replay"
created="${prefix}_created"
renamed="${prefix}_renamed"
missing="${prefix}_missing"
role="${prefix}_role"
profile="${prefix}_profile"
alice_password="InitialPassword_05055"
bob_password="OtherPassword_05055"
denied_password="DeniedPassword_05055"

admin()
{
    $CLICKHOUSE_CLIENT --multiquery --query "$1"
}

as_user()
{
    local user="$1" password="$2" query="$3"
    shift 3
    $CLICKHOUSE_CLIENT --user "$user" --password "$password" --multiquery --query "$query" "$@"
}

cleanup()
{
    admin "DROP USER IF EXISTS $alice, $bob, $replay, $created, $renamed;
           DROP ROLE IF EXISTS $role;
           DROP SETTINGS PROFILE IF EXISTS $profile;" >/dev/null
}
trap cleanup EXIT

fail()
{
    echo "$*" >&2
    exit 1
}

expect_error()
{
    local code="$1" output description
    shift
    description="$1"
    if [[ "$1" == as_user ]]; then
        description="user=$2 query=$4"
    fi
    if output=$("$@" 2>&1); then
        if [[ "$1" == as_user && "$4" == GRANT* ]]; then
            # Show both explicit and effective sources when a delegation check unexpectedly succeeds.
            admin "SHOW GRANTS FOR $2; SHOW GRANTS FOR $2 FINAL WITH IMPLICIT;" >&2
        fi
        fail "Expected $code, but the query succeeded: $description"
    fi
    [[ "$output" == *"($code)"* ]] || fail "Expected $code for $description, received: $output"
}

assert_login()
{
    local result
    result=$(as_user "$1" "$2" "SELECT 1")
    [[ "$result" == 1 ]] || fail "Unexpected login result: $result"
}

definitions()
{
    admin "SHOW CREATE USER $alice; SHOW CREATE USER $bob;"
}

# An access error alone is insufficient: verify definitions and fresh logins too.
deny_change()
{
    local query="$1" before after
    shift
    before=$(definitions)
    expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "$query" "$@"
    after=$(definitions)
    [[ "$before" == "$after" ]] || fail "Denied query changed an account definition"
    assert_login "$alice" "$alice_password"
    assert_login "$bob" "$bob_password"
    expect_error AUTHENTICATION_FAILED as_user "$alice" "$denied_password" "SELECT 1"
    expect_error AUTHENTICATION_FAILED as_user "$bob" "$denied_password" "SELECT 1"
}

assert_query()
{
    local result
    result=$(admin "$1")
    [[ "$result" == "$2" ]] || fail "Expected '$2', received '$result'"
}

admin "CREATE USER $alice IDENTIFIED WITH sha256_password BY '$alice_password';
       CREATE USER $bob IDENTIFIED WITH sha256_password BY '$bob_password';
       CREATE USER $replay IDENTIFIED WITH sha256_password BY '$bob_password';
       CREATE ROLE $role;
       CREATE SETTINGS PROFILE $profile SETTINGS max_threads = 2;"

echo "Explicit permission and privilege metadata"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
admin "GRANT ALTER USER AUTHENTICATION ON $alice TO $alice"
assert_query "SELECT count() FROM system.privileges
              WHERE privilege = 'ALTER USER AUTHENTICATION'
                AND level = 'USER_NAME' AND parent_group = 'ACCESS MANAGEMENT'" 1
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER AUTHENTICATION ON $alice TO $alice" ]] || fail "Incorrect explicit grant: $grants"

echo "Replacement, addition and reset with fresh connections"
old_password="$alice_password"
as_user "$alice" "$alice_password" "ALTER USER \`$alice\` IDENTIFIED BY 'ReplacementPassword_05055'"
alice_password="ReplacementPassword_05055"
assert_login "$alice" "$alice_password"
expect_error AUTHENTICATION_FAILED as_user "$alice" "$old_password" "SELECT 1"
as_user "$alice" "$alice_password" "ALTER USER $alice ADD IDENTIFIED WITH sha256_password BY 'AddedPassword_05055'"
assert_login "$alice" "$alice_password"
assert_login "$alice" "AddedPassword_05055"
as_user "$alice" "$alice_password" "ALTER USER $alice RESET AUTHENTICATION METHODS TO NEW"
expect_error AUTHENTICATION_FAILED as_user "$alice" "$alice_password" "SELECT 1"
alice_password="AddedPassword_05055"
assert_login "$alice" "$alice_password"
# An explicit hash exercises the existing hash validation/conversion path.
password_hash=$(admin "SELECT hex(SHA256('HashPassword_05055'))")
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED WITH sha256_hash BY '$password_hash'"
expect_error AUTHENTICATION_FAILED as_user "$alice" "$alice_password" "SELECT 1"
alice_password="HashPassword_05055"
assert_login "$alice" "$alice_password"

echo "Non-authentication and mixed clauses require full access"
clauses=(
    "HOST ANY"
    "HOST NONE"
    "ADD HOST LOCAL"
    "DROP HOST LOCAL"
    "RENAME TO $renamed"
    "DEFAULT ROLE NONE"
    "DEFAULT ROLE $role"
    "SETTINGS max_threads = 2"
    "SET max_threads = 2"
    "ADD SETTING max_threads = 2"
    "MODIFY SETTING max_threads = 2"
    "DROP SETTING max_threads"
    "DROP ALL SETTINGS"
    "ADD PROFILES '$profile'"
    "DROP PROFILES '$profile'"
    "DROP ALL PROFILES"
    "GRANTEES NONE"
    "DEFAULT DATABASE NONE"
    "VALID UNTIL '2100-01-01'"
)
for clause in "${clauses[@]}"; do
    deny_change "ALTER USER $alice $clause"
    deny_change "ALTER USER $alice $clause IDENTIFIED BY '$denied_password'"
done
deny_change "ALTER USER $alice ADD IDENTIFIED WITH sha256_password BY '$denied_password' HOST NONE"
deny_change "ALTER USER $alice RESET AUTHENTICATION METHODS TO NEW DROP ALL SETTINGS"
# Expiry following IDENTIFIED is method-local; expiry preceding it is global.
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password' VALID UNTIL '2100-01-01'"
deny_change "ALTER USER $alice ADD IDENTIFIED WITH sha256_password BY '$denied_password' VALID UNTIL '2100-01-01'"

echo "Self-service boundary and other account operations"
admin "GRANT ALTER USER AUTHENTICATION ON $bob TO $alice"
deny_change "ALTER USER $bob IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $alice, $bob IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $bob, $alice IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER IF EXISTS $alice, $missing IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER IF EXISTS $missing, $alice IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER IF EXISTS $missing IDENTIFIED BY '$denied_password'"
as_user "$alice" "$alice_password" "ALTER USER IF EXISTS $alice IDENTIFIED BY '$alice_password'"
# Authorization precedes lookup of an explicit storage or cluster, so no fixture is needed.
deny_change "ALTER USER $alice IN nonexistent_self_password_storage IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $alice ON CLUSTER nonexistent_self_password_cluster IDENTIFIED BY '$denied_password'"
deny_change "CREATE USER $created IDENTIFIED BY '$denied_password'"
deny_change "CREATE USER OR REPLACE $alice IDENTIFIED BY '$denied_password'"
deny_change "DROP USER $alice"
deny_change "SET DEFAULT ROLE NONE TO $alice"
assert_query "SELECT count() FROM system.users WHERE name IN ('$created', '$renamed')" 0

echo "Read-only sessions retain credential restrictions"
before=$(definitions)
expect_error READONLY as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$denied_password'" --readonly 1
[[ "$before" == "$(definitions)" ]] || fail "Read-only query changed an account"
assert_login "$alice" "$alice_password"
expect_error AUTHENTICATION_FAILED as_user "$alice" "$denied_password" "SELECT 1"

echo "Implicit full access and independent ordinary revokes"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER ON $alice TO $alice;"
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER ON $alice TO $alice" ]] || fail "Full privilege acquired an explicit authentication grant"
implicit_grants=$(admin "SHOW GRANTS FOR $alice WITH IMPLICIT FORMAT TSVRaw")
[[ "$implicit_grants" == *"ALTER USER AUTHENTICATION"* ]] || fail "Missing implicit authentication privilege"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password' HOST ANY SETTINGS max_threads = 2"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password' VALID UNTIL '2100-01-01'"
admin "GRANT ALTER USER AUTHENTICATION ON $alice TO $alice; REVOKE ALTER USER ON $alice FROM $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $alice HOST ANY"
admin "REVOKE ALTER USER AUTHENTICATION ON $alice FROM $alice"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
admin "GRANT ALTER USER, ALTER USER AUTHENTICATION ON $alice TO $alice;
       REVOKE ALTER USER AUTHENTICATION ON $alice FROM $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"

echo "Prefix, global and group-grant scopes"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER AUTHENTICATION ON ${prefix}* TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $bob IDENTIFIED BY '$denied_password'"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER AUTHENTICATION ON * TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $bob IDENTIFIED BY '$denied_password'"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ACCESS MANAGEMENT ON *.* TO $alice;
       REVOKE ALTER USER ON $alice FROM $alice;"
# A group grant contains the independent authentication leaf even with a full-user exclusion.
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $alice HOST ANY"
admin "REVOKE ALTER USER AUTHENTICATION ON $alice FROM $alice"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
# The test administrator may lack unrelated sensitive privileges, so delegate its grantable rights.
admin "REVOKE ALL ON *.* FROM $alice; GRANT CURRENT GRANTS ON *.* TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"

echo "Enabled roles and scoped implication"
admin "REVOKE ALL ON *.* FROM $alice;
       GRANT ALTER USER ON ${prefix}* TO $role;
       REVOKE ALTER USER ON $alice FROM $role;
       GRANT $role TO $alice;
       ALTER USER $alice DEFAULT ROLE NONE;"
deny_change "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$denied_password'"
admin "GRANT ALTER USER AUTHENTICATION ON $alice TO $role"
as_user "$alice" "$alice_password" "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "SET ROLE NONE; ALTER USER $alice IDENTIFIED BY '$denied_password'"
deny_change "SET ROLE $role; ALTER USER $alice HOST ANY"
admin "GRANT ALTER USER ON $alice TO $role; REVOKE ALTER USER AUTHENTICATION ON $alice FROM $role;"
as_user "$alice" "$alice_password" "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$alice_password'"
admin "GRANT ALTER USER AUTHENTICATION ON $alice TO $alice;
       REVOKE ALTER USER AUTHENTICATION ON $alice FROM $alice;
       GRANT ALTER USER ON ${prefix}* TO $role WITH GRANT OPTION;
       REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $role;"
as_user "$alice" "$alice_password" "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$alice_password'"
as_user "$alice" "$alice_password" "SET ROLE $role; GRANT ALTER USER AUTHENTICATION ON $alice TO $bob"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "SET ROLE $role; GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
admin "GRANT ALTER USER AUTHENTICATION ON $bob TO $role WITH GRANT OPTION"
as_user "$alice" "$alice_password" "SET ROLE $role; GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
admin "REVOKE $role FROM $alice"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"

echo "Delegation and independent grant-option revokes"
admin "GRANT ALTER USER ON $bob TO $alice"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
admin "GRANT ALTER USER ON $bob TO $alice WITH GRANT OPTION"
# The special copy path intersects explicit grants; it must not materialize the implication.
as_user "$alice" "$alice_password" "GRANT CURRENT GRANTS(ALTER USER AUTHENTICATION ON $bob) TO $replay"
assert_query "SELECT count() FROM system.grants WHERE user_name = '$replay' AND access_type = 'ALTER USER AUTHENTICATION'" 0
as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $replay"
assert_query "SELECT count() FROM system.grants WHERE user_name = '$replay' AND access_type = 'ALTER USER AUTHENTICATION'" 1
as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob WITH GRANT OPTION"
as_user "$bob" "$bob_password" "ALTER USER $bob IDENTIFIED BY '$bob_password'"
expect_error ACCESS_DENIED as_user "$bob" "$bob_password" "GRANT ALTER USER ON $bob TO $replay"
as_user "$bob" "$bob_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $replay"
admin "GRANT ALTER USER AUTHENTICATION ON $bob TO $alice WITH GRANT OPTION;
       REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice;"
as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER ON $bob TO $bob"
admin "GRANT ALTER USER ON $bob TO $alice WITH GRANT OPTION;
       REVOKE GRANT OPTION FOR ALTER USER AUTHENTICATION ON $bob FROM $alice;"
as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
admin "REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
# Revoking grant option alone retains ordinary full-user authority.
as_user "$alice" "$alice_password" "ALTER USER $bob IDENTIFIED BY '$bob_password' HOST ANY"

echo "Grant replay preserves explicit scoped rights and grant options"
admin "REVOKE ALL ON *.* FROM $alice; REVOKE ALL ON *.* FROM $replay;
       GRANT ALTER USER ON ${prefix}* TO $alice WITH GRANT OPTION;
       REVOKE ALTER USER ON $alice FROM $alice;
       REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice;
       GRANT ALTER USER AUTHENTICATION ON $alice TO $alice WITH GRANT OPTION;"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == *"REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice"* ]] || fail "Grant export lost the scoped grant-option revoke: $grants"
while IFS= read -r statement; do
    statement="${statement/ TO $alice/ TO $replay}"
    statement="${statement/ FROM $alice/ FROM $replay}"
    admin "$statement"
done <<< "$grants"
replayed_grants=$(admin "SHOW GRANTS FOR $replay FORMAT TSVRaw")
replayed_grants="${replayed_grants// TO $replay/ TO $alice}"
replayed_grants="${replayed_grants// FROM $replay/ FROM $alice}"
[[ "$grants" == "$replayed_grants" ]] || fail "Explicit grants changed during replay"
as_user "$replay" "$bob_password" "GRANT ALTER USER AUTHENTICATION ON $alice TO $bob"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER ON $alice TO $bob"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER AUTHENTICATION ON $bob TO $bob"
admin "REVOKE ALTER USER AUTHENTICATION ON $alice FROM $replay"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER AUTHENTICATION ON $alice TO $bob"
admin "GRANT ALTER USER ON $alice TO $replay WITH GRANT OPTION"
as_user "$replay" "$bob_password" "GRANT ALTER USER AUTHENTICATION ON $alice TO $bob"

echo "No-password replacement retains existing method semantics"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER AUTHENTICATION ON $alice TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice NOT IDENTIFIED"
alice_password=""
assert_login "$alice" "$alice_password"
before=$(definitions)
expect_error BAD_ARGUMENTS as_user "$alice" "$alice_password" "ALTER USER $alice ADD IDENTIFIED WITH sha256_password BY '$denied_password'"
[[ "$before" == "$(definitions)" ]] || fail "Invalid authentication combination changed an account"
assert_login "$alice" "$alice_password"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED WITH sha256_password BY 'FinalPassword_05055'"
alice_password="FinalPassword_05055"
assert_login "$alice" "$alice_password"
expect_error AUTHENTICATION_FAILED as_user "$alice" "" "SELECT 1"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED WITH no_password"
assert_login "$alice" ""

echo "OK"
