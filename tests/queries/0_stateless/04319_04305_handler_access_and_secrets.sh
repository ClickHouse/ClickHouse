#!/usr/bin/env bash
# Tags: no-parallel
# ^ Handlers are a global, server-wide namespace and this test also creates a server-wide user, so it
#   uses fixed names; running several copies concurrently would race on those names.

# Tests access control for the system.handlers introspection table:
#   - reading it requires the SHOW HANDLERS privilege (row filtering);
#   - secrets embedded in the handler query/create_query are masked unless the user is allowed to see
#     them (the test server does not enable display_secrets_in_show_and_select, so they are always masked).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HANDLER="h04319_secret"
USER="u04319"

${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS ${HANDLER}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"

# The query embeds a secret access key/secret. It is only parsed at creation time, never executed.
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER ${HANDLER} URL '/${HANDLER}' AS SELECT * FROM s3('http://example.com/data.csv', 'MYACCESSKEY04319', 'MYSECRETKEY04319', 'CSV')"

echo "--- admin sees the row, but the secret is masked ---"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(countSubstrings(query, 'MYSECRETKEY04319')), sum(countSubstrings(create_query, 'MYSECRETKEY04319')) FROM system.handlers WHERE name = '${HANDLER}'"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} NOT IDENTIFIED"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.handlers TO ${USER}"

echo "--- without SHOW HANDLERS the user sees no rows ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT count() FROM system.handlers WHERE name = '${HANDLER}'"

${CLICKHOUSE_CLIENT} -q "GRANT SHOW HANDLERS ON *.* TO ${USER}"

echo "--- with SHOW HANDLERS the user sees the row, still with the secret masked ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT count(), sum(countSubstrings(query, 'MYSECRETKEY04319')) FROM system.handlers WHERE name = '${HANDLER}'"

${CLICKHOUSE_CLIENT} -q "DROP HANDLER ${HANDLER}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
