#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Quotas and users are server-global, so every name is suffixed with the (unique) database name to
# keep the test isolated when it runs in parallel with itself (e.g. in the flaky check).
user="u_05218_${CLICKHOUSE_DATABASE}"
control_user="uctl_05218_${CLICKHOUSE_DATABASE}"
quota="q_05218_${CLICKHOUSE_DATABASE}"

# Server logs must not reach any client here: a quota recompute that fails is logged at error level
# from the ALTER QUOTA query's own thread, and clickhouse-test fails a shell test on any stderr.
CLICKHOUSE_CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=none/")

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}, ${control_user}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}, ${control_user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO ${user}, ${control_user}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW QUOTAS ON *.* TO ${user}, ${control_user}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY user_name FOR INTERVAL 100 YEAR MAX queries = 1000 TO ${user}, ${control_user}"

# Reads back the quota context of the connecting user, which also builds and caches that context.
# Prints the effective quota key and limit, or only the error name when the query is rejected.
usage() {
    ${CLICKHOUSE_CLIENT} "$@" -q \
        "SELECT replaceOne(quota_key, '_${CLICKHOUSE_DATABASE}', ''), max_queries FROM system.quota_usage WHERE quota_name = '${quota}'" 2>&1 \
        | tr '\t' ' ' | grep -oE "QUOTA_REQUIRES_CLIENT_KEY|^[[:alnum:]_]+ [[:digit:]]+$"
}

echo "1. warm two cached quota contexts of the same user, without and with a quota key"
usage --user "${user}"
usage --user "${user}" --quota_key kb

echo "2. a tightening that keeps the key type reaches the warm context"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${quota} FOR INTERVAL 100 YEAR MAX queries = 900"
usage --user "${user}"

echo "3. switching the key type to client_key must reach both warm contexts"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${quota} KEYED BY client_key FOR INTERVAL 100 YEAR MAX queries = 800"
usage --user "${user}"
usage --user "${user}" --quota_key kb

echo "4. a user whose context was never cached is rejected the same way"
usage --user "${control_user}"

echo "5. reverting the key type lets the never-detached context resume"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${quota} KEYED BY user_name FOR INTERVAL 100 YEAR MAX queries = 700"
usage --user "${user}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${user}, ${control_user}"
