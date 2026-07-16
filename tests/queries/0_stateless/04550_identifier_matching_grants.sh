#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Access control under database_and_table_name_matching = 'standard': names are canonicalized in
# Context::resolveStorageID before access checks, so grants apply to the canonical object.
DB="${CLICKHOUSE_DATABASE}_AclDb"
DB_FOLDED=$(echo "$DB" | tr '[:upper:]' '[:lower:]')
ACL_USER="u_04518_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${ACL_USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.Granted (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.Granted VALUES (7)"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.Denied (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.Foo (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.FOO (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${ACL_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.Granted TO ${ACL_USER}"

AS_USER="${CLICKHOUSE_CLIENT} --user ${ACL_USER}"
AS_USER_STANDARD="${AS_USER} --database_and_table_name_matching=standard"

echo '--- standard: folded reference to the granted table resolves canonically, access passes'
${AS_USER_STANDARD} --query "SELECT x FROM ${DB_FOLDED}.granted"

echo '--- standard: folded reference to an ungranted existing table is denied, not unknown'
${AS_USER_STANDARD} --query "SELECT x FROM ${DB_FOLDED}.denied" 2>&1 | grep -oF "ACCESS_DENIED" | uniq

echo '--- standard: ambiguity is permission-independent, same error for user and admin'
# Candidate names in the ambiguity error are intentionally not redacted, consistent with the
# server's existing name hints ("Maybe you meant").
${AS_USER_STANDARD} --query "SELECT x FROM ${DB_FOLDED}.foo" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SELECT x FROM ${DB_FOLDED}.foo" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: grants do not steer resolution, folded sibling class stays ambiguous'
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.FOO TO ${ACL_USER}"
${AS_USER_STANDARD} --query "SELECT x FROM ${DB_FOLDED}.foo" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${AS_USER_STANDARD} --query "SELECT count() FROM ${DB_FOLDED}.\"FOO\""
${AS_USER_STANDARD} --query "SELECT count() FROM ${DB_FOLDED}.\"Foo\"" 2>&1 | grep -oF "ACCESS_DENIED" | uniq

echo '--- sensitive: exact names behave as master for the same user'
${AS_USER} --query "SELECT x FROM ${DB}.Granted"
${AS_USER} --query "SELECT x FROM ${DB}.Denied" 2>&1 | grep -oF "ACCESS_DENIED" | uniq
${AS_USER} --query "SELECT x FROM ${DB}.granted" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
