#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL='fatal'

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="test_05219_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT "
CREATE TABLE t_mt (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_mt VALUES ('x');
CREATE TABLE t_mem (s String) ENGINE = Memory;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;
"

# Without CHECK privilege every variant must fail with ACCESS_DENIED and nothing else:
# not the missing part, not the existing part, not the unsupported engine, not the missing table.
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_9_9_9'" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_1_1_0'" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mem" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_missing" 2>&1 | grep -o -F 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT "GRANT CHECK ON ${CLICKHOUSE_DATABASE}.* TO $user"

$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_1_1_0' SETTINGS check_query_single_value_result = 1"
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_9_9_9'" 2>&1 | grep -o -F 'NO_SUCH_DATA_PART'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mem" 2>&1 | grep -o -F 'NOT_IMPLEMENTED'

$CLICKHOUSE_CLIENT "
DROP USER $user;
DROP TABLE t_mt;
DROP TABLE t_mem;
"
