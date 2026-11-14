#!/usr/bin/env bash
# Tags: no-fasttest

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
    elif [[ "$1" == admin ]]; then
        description="query=$2"
    fi
    if output=$("$@" 2>&1); then
        if [[ "$1" == as_user && "${4:-}" == GRANT* ]]; then
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
admin "GRANT ALTER USER SELF AUTHENTICATION TO $alice"
assert_query "SELECT count() FROM system.privileges
              WHERE privilege = 'ALTER USER SELF AUTHENTICATION'
                AND level = 'GLOBAL' AND parent_group = 'ACCESS MANAGEMENT'" 1
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER SELF AUTHENTICATION TO $alice" ]] || fail "Incorrect explicit grant: $grants"

echo "Short syntax, mixed groups and global scope validation"
for verb in "GRANT" "REVOKE"; do
    recipient="TO"
    [[ "$verb" != REVOKE ]] || recipient="FROM"
    expect_error SYNTAX_ERROR admin "$verb ALTER USER SELF AUTHENTICATION ON $bob $recipient $replay"
    expect_error SYNTAX_ERROR admin "$verb ALTER USER SELF AUTHENTICATION ON *.* $recipient $replay"
    expect_error INVALID_GRANT admin "$verb SELECT, ALTER USER SELF AUTHENTICATION ON ${CLICKHOUSE_DATABASE}.a_table $recipient $replay"
done
expect_error SYNTAX_ERROR admin "GRANT ALTER USER SELF AUTHENTICATION(x) ON *.* TO $replay"
expect_error SYNTAX_ERROR admin "GRANT ALTER USER SELF AUTHENTICATION, SELECT TO $replay"
expect_error SYNTAX_ERROR admin "GRANT CREATE USER TO $replay"
assert_query "SELECT position(formatQuery('GRANT ALTER USER SELF AUTHENTICATION ON CLUSTER a_cluster TO $replay'), 'ON CLUSTER') > 0" 1
admin "GRANT SELECT, ALTER USER SELF AUTHENTICATION ON *.* TO $replay"
grants=$(admin "SHOW GRANTS FOR $replay FORMAT TSVRaw")
[[ "$grants" == *" ON *.* TO $replay"* ]] || fail "Mixed global grants lost their scope: $grants"
admin "REVOKE ALL ON *.* FROM $replay; GRANT CURRENT GRANTS(ALTER USER SELF AUTHENTICATION) TO $replay;"
grants=$(admin "SHOW GRANTS FOR $replay FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER SELF AUTHENTICATION TO $replay" ]] || fail "Incorrect current-grants copy: $grants"
admin "REVOKE ALL ON *.* FROM $replay"

echo "Mixed parameter types retain formatting round trips"
for privileges in "ALTER USER SELF AUTHENTICATION, ALTER USER" "ALTER USER, ALTER USER SELF AUTHENTICATION"; do
    statements=(
        "GRANT $privileges ON *.* TO $replay"
        "GRANT $privileges ON *.* TO $replay WITH GRANT OPTION"
        "REVOKE $privileges ON *.* FROM $replay"
        "REVOKE GRANT OPTION FOR $privileges ON *.* FROM $replay"
        "GRANT CURRENT GRANTS($privileges ON *.*) TO $replay"
        "GRANT $privileges ON *.* TO $replay ON CLUSTER a_cluster"
    )
    for statement in "${statements[@]}"; do
        formatted=$(admin "SELECT formatQuerySingleLine('$statement')")
        reformatted=$(admin "SELECT formatQuerySingleLine('$formatted')")
        [[ "$formatted" == "$reformatted" ]] || fail "Formatting is not stable: $statement"
    done
    # Execute the formatted grant, then check both independent permissions at global scope.
    formatted=$(admin "SELECT formatQuerySingleLine('GRANT $privileges ON *.* TO $replay WITH GRANT OPTION')")
    admin "$formatted"
    assert_query "SELECT count() FROM system.grants WHERE user_name = '$replay'
                  AND access_type IN ('ALTER USER', 'ALTER USER SELF AUTHENTICATION')
                  AND access_object = '' AND grant_option = 1 AND is_partial_revoke = 0" 2
    admin "REVOKE ALL ON *.* FROM $replay"
done

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
deny_change "ALTER USER $bob IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $alice, $bob IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $bob, $alice IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER IF EXISTS $alice, $missing IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER IF EXISTS $missing, $alice IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER IF EXISTS $missing IDENTIFIED BY '$denied_password'"
as_user "$alice" "$alice_password" "ALTER USER IF EXISTS $alice IDENTIFIED BY '$alice_password'"
# Account/storage authorization precedes lookup of an explicit storage or cluster.
deny_change "ALTER USER $alice IN nonexistent_self_password_storage IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $bob ON CLUSTER nonexistent_self_password_cluster IDENTIFIED BY '$denied_password'"
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

echo "Full user access and independent ordinary revokes"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER ON $alice TO $alice;"
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER ON $alice TO $alice" ]] || fail "Full privilege acquired an explicit self-service grant"
implicit_grants=$(admin "SHOW GRANTS FOR $alice WITH IMPLICIT FORMAT TSVRaw")
[[ "$implicit_grants" != *"ALTER USER SELF AUTHENTICATION"* ]] || fail "Scoped full access implied a global self-service privilege"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password' HOST ANY SETTINGS max_threads = 2"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password' VALID UNTIL '2100-01-01'"
admin "GRANT ALTER USER SELF AUTHENTICATION TO $alice; REVOKE ALTER USER ON $alice FROM $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $alice HOST ANY"
admin "REVOKE ALTER USER SELF AUTHENTICATION FROM $alice"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
admin "GRANT ALTER USER ON $alice TO $alice;
       GRANT ALTER USER SELF AUTHENTICATION TO $alice;
       REVOKE ALTER USER SELF AUTHENTICATION FROM $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"

echo "Global self-service and administrative scopes"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER ON $bob TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $bob IDENTIFIED BY '$bob_password' HOST ANY"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER SELF AUTHENTICATION TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $bob IDENTIFIED BY '$denied_password'"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ACCESS MANAGEMENT ON *.* TO $alice;
       REVOKE ALTER USER ON $alice FROM $alice;"
# A group grant contains the independent self-service leaf even with a full-user exclusion.
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
deny_change "ALTER USER $alice HOST ANY"
admin "REVOKE ALTER USER SELF AUTHENTICATION FROM $alice"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
# The test administrator may lack unrelated sensitive privileges, so delegate its grantable rights.
admin "REVOKE ALL ON *.* FROM $alice; GRANT CURRENT GRANTS ON *.* TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"

echo "Enabled roles supply each member's self-service permission"
admin "REVOKE ALL ON *.* FROM $alice;
       GRANT ALTER USER SELF AUTHENTICATION TO $role;
       GRANT $role TO $alice, $bob;
       ALTER USER $alice DEFAULT ROLE NONE;
       ALTER USER $bob DEFAULT ROLE NONE;"
as_user "$alice" "$alice_password" "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$alice_password'"
as_user "$bob" "$bob_password" "SET ROLE $role; ALTER USER $bob IDENTIFIED BY '$bob_password'"
deny_change "SET ROLE NONE; ALTER USER $alice IDENTIFIED BY '$denied_password'"
deny_change "SET ROLE $role; ALTER USER $bob IDENTIFIED BY '$denied_password'"
deny_change "SET ROLE $role; ALTER USER $alice HOST ANY"
admin "GRANT ALTER USER SELF AUTHENTICATION TO $alice;
       REVOKE ALTER USER SELF AUTHENTICATION FROM $alice;
       GRANT ALTER USER ON $bob TO $role WITH GRANT OPTION;"
as_user "$alice" "$alice_password" "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$alice_password'"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "SET ROLE $role; GRANT ALTER USER SELF AUTHENTICATION TO $replay"
admin "GRANT ALTER USER SELF AUTHENTICATION TO $role WITH GRANT OPTION"
as_user "$alice" "$alice_password" "SET ROLE $role; GRANT ALTER USER SELF AUTHENTICATION TO $replay"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "SET ROLE NONE; GRANT ALTER USER SELF AUTHENTICATION TO $replay"
admin "REVOKE $role FROM $alice, $bob; REVOKE ALL ON *.* FROM $replay;"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"

echo "Independent delegation and grant-option revokes"
# Even full user-administration grant option on every account does not include the self-service bit.
admin "GRANT ALTER USER ON * TO $alice WITH GRANT OPTION"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
admin "REVOKE ALL ON *.* FROM $alice"
admin "GRANT ALTER USER ON $bob TO $alice WITH GRANT OPTION"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
as_user "$alice" "$alice_password" "GRANT CURRENT GRANTS(ALTER USER SELF AUTHENTICATION) TO $replay"
assert_query "SELECT count() FROM system.grants WHERE user_name = '$replay' AND access_type = 'ALTER USER SELF AUTHENTICATION'" 0
admin "GRANT ALTER USER SELF AUTHENTICATION TO $alice WITH GRANT OPTION"
as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob WITH GRANT OPTION"
as_user "$bob" "$bob_password" "ALTER USER $bob IDENTIFIED BY '$bob_password'"
expect_error ACCESS_DENIED as_user "$bob" "$bob_password" "GRANT ALTER USER ON $bob TO $replay"
as_user "$bob" "$bob_password" "GRANT ALTER USER SELF AUTHENTICATION TO $replay"
admin "REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice"
as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER ON $bob TO $bob"
admin "GRANT ALTER USER ON $bob TO $alice WITH GRANT OPTION;
       REVOKE GRANT OPTION FOR ALTER USER SELF AUTHENTICATION FROM $alice;"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
# Revoking grant option alone retains ordinary self-service and administrative authority.
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
as_user "$alice" "$alice_password" "ALTER USER $bob IDENTIFIED BY '$bob_password' HOST ANY"
admin "REVOKE ALL ON *.* FROM $replay;
       GRANT ALTER USER SELF AUTHENTICATION TO $alice WITH GRANT OPTION;"
as_user "$alice" "$alice_password" "GRANT CURRENT GRANTS(ALTER USER SELF AUTHENTICATION) TO $replay"
assert_query "SELECT count() FROM system.grants WHERE user_name = '$replay' AND access_type = 'ALTER USER SELF AUTHENTICATION'" 1
admin "REVOKE ALTER USER SELF AUTHENTICATION FROM $alice"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"

echo "Grant replay preserves explicit scoped rights and grant options"
admin "REVOKE ALL ON *.* FROM $alice; REVOKE ALL ON *.* FROM $replay;
       GRANT ALTER USER ON ${prefix}* TO $alice WITH GRANT OPTION;
       REVOKE ALTER USER ON $alice FROM $alice;
       REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice;
       GRANT ALTER USER SELF AUTHENTICATION TO $alice WITH GRANT OPTION;"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER ON $bob TO $bob"
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
as_user "$replay" "$bob_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER ON $alice TO $bob"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER ON $bob TO $bob"
admin "REVOKE ALTER USER SELF AUTHENTICATION FROM $replay"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
# A scoped full-user grant option still cannot delegate the independent global permission.
admin "GRANT ALTER USER ON $alice TO $replay WITH GRANT OPTION"
expect_error ACCESS_DENIED as_user "$replay" "$bob_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
admin "GRANT ALTER USER SELF AUTHENTICATION TO $replay WITH GRANT OPTION"
as_user "$replay" "$bob_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"

echo "Renaming an account preserves its self-service permission"
admin "REVOKE ALL ON *.* FROM $alice;
       GRANT ALTER USER SELF AUTHENTICATION TO $alice;
       ALTER USER $alice RENAME TO $renamed;"
as_user "$renamed" "$alice_password" "ALTER USER $renamed IDENTIFIED BY '$alice_password'"
admin "ALTER USER $renamed RENAME TO $alice"
assert_login "$alice" "$alice_password"

echo "No-password replacement retains existing method semantics"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER SELF AUTHENTICATION TO $alice;"
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
