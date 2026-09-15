#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# DNSError is a ProfileEvent, so attribute it to each remote() query via query_log instead of the
# process-wide system.events counter. This is immune to concurrent DNS activity from other tests.
query_id_1="${CLICKHOUSE_TEST_UNIQUE_NAME}_1"
${CLICKHOUSE_CLIENT} --query_id "$query_id_1" --query "SELECT * FROM remote('ThisHostNameDoesNotExistSoItShouldFail', system, one)" 2>/dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "SELECT 'first_check', ProfileEvents['DNSError'] > 0 FROM system.query_log WHERE query_id = '$query_id_1' AND type != 'QueryStart' AND current_database = currentDatabase()"

query_id_2="${CLICKHOUSE_TEST_UNIQUE_NAME}_2"
${CLICKHOUSE_CLIENT} --query_id "$query_id_2" --query "SELECT * FROM remote('ThisHostNameDoesNotExistSoItShouldFail2', system, one)" 2>/dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "SELECT 'second_check', ProfileEvents['DNSError'] > 0 FROM system.query_log WHERE query_id = '$query_id_2' AND type != 'QueryStart' AND current_database = currentDatabase()"
