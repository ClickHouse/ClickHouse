#!/usr/bin/env bash
# Tags: no-parallel
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS user_03234; DROP TABLE IF EXISTS ${db}.tb;CREATE USER user_03234; GRANT SELECT ON ${db}.tb TO user_03234;"

# Has been granted but not table not exists 
# expected to 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=user_03234" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.tb (\`content\` UInt64) ENGINE = MergeTree ORDER BY content; INSERT INTO ${db}.tb VALUES (1);"
# Has been granted and table exists
# expected to 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=user_03234" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${db}.tb FROM user_03234;"
# Has not been granted but table exists
# expected to 0
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=user_03234" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

# Role
# expected to 1
${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS role_03234;CREATE ROLE role_03234;GRANT SELECT ON ${db}.tb TO role_03234;GRANT role_03234 TO user_03234"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=user_03234" --data-binary "SET ROLE role_03234"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=user_03234" --data-binary "CHECK GRANT SELECT ON ${db}.tb"

# wildcard
# expected to 1
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.tbb* TO user_03234;"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=user_03234" --data-binary "CHECK GRANT SELECT ON ${db}.tbb1"
