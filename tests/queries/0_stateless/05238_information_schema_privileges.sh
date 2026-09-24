#!/usr/bin/env bash
# Tags: log-engine
# (log-engine: `GRANT TABLE ENGINE ON TinyLog` throws UNKNOWN_STORAGE on builds without TinyLog registered)
# INFORMATION_SCHEMA *_PRIVILEGES views (issue #29068): each grant level is routed
# into exactly one view; partial revokes, wildcard and parameterized grants are hidden.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_05238_${db}"
role="role_05238_${db}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS $role"
${CLICKHOUSE_CLIENT} -q "CREATE USER $user"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE $role"

${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON *.* TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $db.* TO $user WITH GRANT OPTION"
${CLICKHOUSE_CLIENT} -q "GRANT ALTER UPDATE ON $db.t TO $role"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT(a, b) ON $db.t TO $role"

# A quote and a backslash in the grantee name must be escaped MySQL-style:
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS 'q''\\\\05238_${db}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER 'q''\\\\05238_${db}'"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE ON *.* TO 'q''\\\\05238_${db}'"

# Not representable in MySQL terms, must not appear in any of the views:
${CLICKHOUSE_CLIENT} -q "REVOKE SHOW TABLES ON secret_05238.* FROM $user"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON wildcard_05238*.* TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON TinyLog TO $user"

echo "-- user_privileges"
${CLICKHOUSE_CLIENT} -q "SELECT replaceAll(grantee, '$db', '[db]'), table_catalog, privilege_type, is_grantable
    FROM information_schema.user_privileges WHERE grantee LIKE '%_05238_$db%' ORDER BY ALL"

echo "-- schema_privileges"
${CLICKHOUSE_CLIENT} -q "SELECT replaceAll(grantee, '$db', '[db]'), table_catalog, replaceAll(table_schema, '$db', '[db]'), privilege_type, is_grantable
    FROM information_schema.schema_privileges WHERE grantee LIKE '%_05238_$db%' ORDER BY ALL"

echo "-- table_privileges"
${CLICKHOUSE_CLIENT} -q "SELECT replaceAll(grantee, '$db', '[db]'), table_catalog, replaceAll(table_schema, '$db', '[db]'), table_name, privilege_type, is_grantable
    FROM information_schema.table_privileges WHERE grantee LIKE '%_05238_$db%' ORDER BY ALL"

echo "-- column_privileges"
${CLICKHOUSE_CLIENT} -q "SELECT replaceAll(grantee, '$db', '[db]'), table_catalog, replaceAll(table_schema, '$db', '[db]'), table_name, column_name, privilege_type, is_grantable
    FROM information_schema.column_privileges WHERE grantee LIKE '%_05238_$db%' ORDER BY ALL"

echo "-- grantee with a quote and a backslash in the name"
${CLICKHOUSE_CLIENT} -q "SELECT replaceAll(grantee, '$db', '[db]'), privilege_type FROM information_schema.user_privileges
    WHERE privilege_type = 'CREATE TEMPORARY TABLE' AND grantee LIKE '%05238_$db%'"

echo "-- not representable grants are hidden"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM information_schema.user_privileges WHERE grantee LIKE '%_05238_$db%' AND privilege_type = 'TABLE ENGINE'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM information_schema.schema_privileges WHERE table_schema LIKE 'secret_05238%' OR table_schema LIKE 'wildcard_05238%'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM information_schema.table_privileges WHERE grantee LIKE '%_05238_$db%' AND table_name != 't'"

echo "-- upper/lowercase table names and columns"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM INFORMATION_SCHEMA.USER_PRIVILEGES WHERE GRANTEE LIKE '%user_05238_$db%' AND PRIVILEGE_TYPE = 'SELECT'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM information_schema.USER_PRIVILEGES WHERE GRANTEE LIKE '%user_05238_$db%' AND PRIVILEGE_TYPE = 'SELECT'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM INFORMATION_SCHEMA.user_privileges WHERE grantee LIKE '%user_05238_$db%' AND privilege_type = 'SELECT'"

${CLICKHOUSE_CLIENT} -q "DROP USER $user"
${CLICKHOUSE_CLIENT} -q "DROP USER 'q''\\\\05238_${db}'"
${CLICKHOUSE_CLIENT} -q "DROP ROLE $role"
