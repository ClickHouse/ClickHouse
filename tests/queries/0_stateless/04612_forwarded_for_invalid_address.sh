#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

test_user="xff_invalid_user_${CLICKHOUSE_DATABASE}"
test_quota="xff_invalid_quota_${CLICKHOUSE_DATABASE}"

# Before validation, the `SocketAddress` constructor resolved this hostname before checking the password.
# The random numeric port makes the warning unique without involving service-name resolution.
invalid_xff="LOCALHOST:$((10000 + RANDOM % 50000))"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP QUOTA IF EXISTS ${test_quota};
        DROP USER IF EXISTS ${test_user};
    " >/dev/null 2>&1 || true
}

trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    DROP QUOTA IF EXISTS ${test_quota};
    DROP USER IF EXISTS ${test_user};

    CREATE USER ${test_user} IDENTIFIED WITH plaintext_password BY 'correct_password';
    CREATE QUOTA ${test_quota}
        KEYED BY forwarded_ip_address
        FOR INTERVAL 1 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 10
        TO ${test_user};
"

log_start_time="$(${CLICKHOUSE_CLIENT} --query "SELECT now64(6)")"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" \
    -H "X-Forwarded-For: ${invalid_xff}" \
    -H "X-ClickHouse-User: ${test_user}" \
    -H "X-ClickHouse-Key: wrong_password" \
    --data-binary "SELECT 1" \
    | grep -m1 -o 'Authentication failed'

# A rejected value must not reappear as a resolved address in the `forwarded_ip_address` quota key.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() = 1 AND countIf(quota_key = '') = 1
    FROM system.quotas_usage
    WHERE quota_name = '${test_quota}'
    FORMAT TSV
"

# The warning is written asynchronously after the HTTP response is sent, so retry until it appears.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    warning_found=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() > 0
        FROM system.text_log
        WHERE event_time_microseconds >= toDateTime64('${log_start_time}', 6)
            AND logger_name = 'ClientInfo'
            AND level = 'Warning'
            AND position(message, 'Invalid address in') > 0
            AND position(message, 'X-Forwarded-For') > 0
            AND position(message, '${invalid_xff}') > 0
        SETTINGS max_rows_to_read = 0
        FORMAT TSV
    ")
    [ "${warning_found}" = "1" ] && break
    sleep 0.5
done

echo "${warning_found}"
