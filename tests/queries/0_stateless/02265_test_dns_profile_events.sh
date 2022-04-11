#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


current_dns_errors=$($CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events where event = 'DNSError';")
${CLICKHOUSE_CLIENT} --query "SELECT * FROM remote('ThisHostNameDoesNotExistSoItShouldFail', system, one)" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'first_check', sum(value) > ${current_dns_errors} FROM system.events where event = 'DNSError';"

current_dns_errors=$($CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events where event = 'DNSError';")
${CLICKHOUSE_CLIENT} --query "SELECT * FROM remote('ThisHostNameDoesNotExistSoItShouldFail2', system, one)" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'second_check', sum(value) > ${current_dns_errors} FROM system.events where event = 'DNSError';"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP DNS CACHE"
