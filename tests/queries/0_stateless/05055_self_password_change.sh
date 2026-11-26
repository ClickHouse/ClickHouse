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
           DROP ROLE IF EXISTS $role;" >/dev/null
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

deny()
{
    expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "$@"
}

# An access error alone is insufficient: verify definitions and fresh logins too.
deny_change()
{
    local query="$1" before after
    shift
    before=$(definitions)
    deny "$query" "$@"
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
       CREATE ROLE $role;"

echo "Explicit permission"
deny_change "ALTER USER $alice IDENTIFIED BY '$denied_password'"
admin "GRANT ALTER USER SELF AUTHENTICATION TO $alice"
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER SELF AUTHENTICATION TO $alice" ]] || fail "Incorrect explicit grant: $grants"

echo "Self-service alone does not authorize CREATE TOKEN"
deny_change "CREATE TOKEN SETTINGS create_token_default_ttl_seconds = 0"

echo "Short syntax, mixed groups and global scope validation"
expect_error SYNTAX_ERROR admin "GRANT ALTER USER SELF AUTHENTICATION ON $bob TO $replay"
expect_error SYNTAX_ERROR admin "GRANT ALTER USER SELF AUTHENTICATION ON *.* TO $replay"
expect_error INVALID_GRANT admin "GRANT SELECT, ALTER USER SELF AUTHENTICATION ON ${CLICKHOUSE_DATABASE}.a_table TO $replay"
expect_error SYNTAX_ERROR admin "GRANT ALTER USER SELF AUTHENTICATION(x) ON *.* TO $replay"
expect_error SYNTAX_ERROR admin "GRANT ALTER USER SELF AUTHENTICATION, SELECT TO $replay"
expect_error SYNTAX_ERROR admin "GRANT CREATE USER TO $replay"
admin "GRANT SELECT, ALTER USER SELF AUTHENTICATION ON *.* TO $replay"
grants=$(admin "SHOW GRANTS FOR $replay FORMAT TSVRaw")
[[ "$grants" == *" ON *.* TO $replay"* ]] || fail "Mixed global grants lost their scope: $grants"
admin "REVOKE ALL ON *.* FROM $replay; GRANT CURRENT GRANTS(ALTER USER SELF AUTHENTICATION) TO $replay;"
grants=$(admin "SHOW GRANTS FOR $replay FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER SELF AUTHENTICATION TO $replay" ]] || fail "Incorrect current-grants copy: $grants"
admin "REVOKE ALL ON *.* FROM $replay"

echo "Formatted mixed parameter types grant both permissions"
for privileges in "ALTER USER SELF AUTHENTICATION, ALTER USER" "ALTER USER, ALTER USER SELF AUTHENTICATION"; do
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

echo "Account-property clauses combined with authentication require full access"
# One clause per account-property field of the query.
clauses=(
    "HOST NONE"
    "ADD HOST LOCAL"
    "DROP HOST LOCAL"
    "RENAME TO $renamed"
    "DEFAULT ROLE NONE"
    "SETTINGS max_threads = 2"
    "MODIFY SETTING max_threads = 2"
    "GRANTEES NONE"
    "DEFAULT DATABASE NONE"
    "VALID UNTIL '2100-01-01'"
)
deny_change "ALTER USER $alice HOST NONE IDENTIFIED BY '$denied_password'"
for clause in "${clauses[@]}"; do
    deny "ALTER USER $alice $clause IDENTIFIED BY '$denied_password'"
done
deny "ALTER USER $alice ADD IDENTIFIED WITH sha256_password BY '$denied_password' HOST NONE"
deny "ALTER USER $alice RESET AUTHENTICATION METHODS TO NEW DROP ALL SETTINGS"

echo "Self-service boundary and other account operations"
deny_change "ALTER USER $bob IDENTIFIED BY '$denied_password'"
deny_change "ALTER USER $alice, $bob IDENTIFIED BY '$denied_password'"
deny "ALTER USER IF EXISTS $alice, $missing IDENTIFIED BY '$denied_password'"
deny "ALTER USER IF EXISTS $missing IDENTIFIED BY '$denied_password'"
as_user "$alice" "$alice_password" "ALTER USER IF EXISTS $alice IDENTIFIED BY '$alice_password'"
# Account/storage authorization precedes lookup of an explicit storage or cluster.
deny "ALTER USER $alice IN nonexistent_self_password_storage IDENTIFIED BY '$denied_password'"
deny "ALTER USER $bob ON CLUSTER nonexistent_self_password_cluster IDENTIFIED BY '$denied_password'"
deny "CREATE USER $created IDENTIFIED BY '$denied_password'"
deny "CREATE USER OR REPLACE $alice IDENTIFIED BY '$denied_password'"
assert_query "SELECT count() FROM system.users WHERE name IN ('$created', '$renamed')" 0

echo "Read-only sessions retain credential restrictions"
before=$(definitions)
expect_error READONLY as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$denied_password'" --readonly 1
[[ "$before" == "$(definitions)" ]] || fail "Read-only query changed an account"
assert_login "$alice" "$alice_password"
expect_error AUTHENTICATION_FAILED as_user "$alice" "$denied_password" "SELECT 1"

echo "Full user access and self-service are independent"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER ON $alice TO $alice;"
grants=$(admin "SHOW GRANTS FOR $alice FORMAT TSVRaw")
[[ "$grants" == "GRANT ALTER USER ON $alice TO $alice" ]] || fail "Full privilege acquired an explicit self-service grant"
implicit_grants=$(admin "SHOW GRANTS FOR $alice WITH IMPLICIT FORMAT TSVRaw")
[[ "$implicit_grants" != *"ALTER USER SELF AUTHENTICATION"* ]] || fail "Scoped full access implied a global self-service privilege"
admin "GRANT ALTER USER SELF AUTHENTICATION TO $alice; REVOKE ALTER USER ON $alice FROM $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"

echo "Access management group includes self-service"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ACCESS MANAGEMENT ON *.* TO $alice;
       REVOKE ALTER USER ON $alice FROM $alice;"
# A group grant contains the independent self-service leaf even with a full-user exclusion.
as_user "$alice" "$alice_password" "ALTER USER $alice IDENTIFIED BY '$alice_password'"
admin "REVOKE ALTER USER SELF AUTHENTICATION FROM $alice"
deny "ALTER USER $alice IDENTIFIED BY '$denied_password'"

echo "Enabled roles supply each member's self-service permission"
admin "REVOKE ALL ON *.* FROM $alice;
       GRANT ALTER USER SELF AUTHENTICATION TO $role;
       GRANT $role TO $alice, $bob;
       ALTER USER $alice DEFAULT ROLE NONE;
       ALTER USER $bob DEFAULT ROLE NONE;"
as_user "$alice" "$alice_password" "SET ROLE $role; ALTER USER $alice IDENTIFIED BY '$alice_password'"
as_user "$bob" "$bob_password" "SET ROLE $role; ALTER USER $bob IDENTIFIED BY '$bob_password'"
deny "SET ROLE NONE; ALTER USER $alice IDENTIFIED BY '$denied_password'"
deny_change "SET ROLE $role; ALTER USER $bob IDENTIFIED BY '$denied_password'"
admin "REVOKE $role FROM $alice, $bob"
deny "ALTER USER $alice IDENTIFIED BY '$denied_password'"

echo "Delegation requires the self-service grant option"
# Even full user-administration grant option on every account does not include the self-service bit.
admin "GRANT ALTER USER ON * TO $alice WITH GRANT OPTION"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER SELF AUTHENTICATION TO $alice WITH GRANT OPTION;"
as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $bob"
as_user "$bob" "$bob_password" "ALTER USER $bob IDENTIFIED BY '$bob_password'"
admin "REVOKE GRANT OPTION FOR ALTER USER SELF AUTHENTICATION FROM $alice"
expect_error ACCESS_DENIED as_user "$alice" "$alice_password" "GRANT ALTER USER SELF AUTHENTICATION TO $replay"
admin "REVOKE ALL ON *.* FROM $bob"

echo "Grant replay preserves explicit scoped rights and grant options"
admin "REVOKE ALL ON *.* FROM $alice; REVOKE ALL ON *.* FROM $replay;
       GRANT ALTER USER ON ${prefix}* TO $alice WITH GRANT OPTION;
       REVOKE ALTER USER ON $alice FROM $alice;
       REVOKE GRANT OPTION FOR ALTER USER ON $bob FROM $alice;
       GRANT ALTER USER SELF AUTHENTICATION TO $alice WITH GRANT OPTION;"
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

echo "No-password replacement"
admin "REVOKE ALL ON *.* FROM $alice; GRANT ALTER USER SELF AUTHENTICATION TO $alice;"
as_user "$alice" "$alice_password" "ALTER USER $alice NOT IDENTIFIED"
assert_login "$alice" ""

echo "OK"
