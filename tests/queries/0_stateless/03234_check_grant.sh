#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user=user_${CLICKHOUSE_TEST_UNIQUE_NAME}
role=role_${CLICKHOUSE_TEST_UNIQUE_NAME}

${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS $role; DROP USER IF EXISTS $user; DROP TABLE IF EXISTS ${db}.tb;"

${CLICKHOUSE_CLIENT} --query "CREATE USER $user; GRANT SELECT ON ${db}.tb TO $user;"

# Has been granted but not table not exists 
# expected to 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.tb (\`content\` UInt64) ENGINE = MergeTree ORDER BY content; INSERT INTO ${db}.tb VALUES (1);"
# Has been granted and table exists
# expected to 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${db}.tb FROM $user;"
# Has not been granted but table exists
# expected to 0
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

# Role
# expected to 1
${CLICKHOUSE_CLIENT} --query "CREATE ROLE $role; GRANT SELECT ON ${db}.tb TO $role; GRANT $role TO $user"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "SET ROLE $role"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

# wildcard
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.tbb* TO $user;"
# expected to 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.tbb1"
# expected to 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.tbb2*"

${CLICKHOUSE_CLIENT} --query "DROP ROLE $role; DROP USER $user; DROP TABLE $db.tb;"
