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
GRANT SHOW DATABASES ON *.* TO $user;
"

# Without CHECK privilege every variant must fail with ACCESS_DENIED and nothing else:
# not the missing part, not the existing part, not the unsupported engine, not the missing table,
# not the missing database.
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_9_9_9'" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_1_1_0'" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mem" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_missing" 2>&1 | grep -o -F 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE db_missing_05219.t" 2>&1 | grep -o -F 'ACCESS_DENIED'

# CHECK ALL TABLES skips the tables the user may not CHECK instead of failing on the first of them,
# so without the privilege it checks nothing and reports no error about any table of any database.
$CLICKHOUSE_CLIENT --user "$user" "CHECK ALL TABLES SETTINGS check_query_single_value_result = 0" 2>&1 | wc -l

$CLICKHOUSE_CLIENT "GRANT CHECK ON ${CLICKHOUSE_DATABASE}.* TO $user"

$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_1_1_0' SETTINGS check_query_single_value_result = 1"
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mt PART 'all_9_9_9'" 2>&1 | grep -o -F 'NO_SUCH_DATA_PART'
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_mem" 2>&1 | grep -o -F 'NOT_IMPLEMENTED'
# Only the tables of the granted database are checked.
$CLICKHOUSE_CLIENT --user "$user" "CHECK ALL TABLES SETTINGS check_query_single_value_result = 0" 2>&1 | sed "s/^${CLICKHOUSE_DATABASE}\t/db\t/"

$CLICKHOUSE_CLIENT "
DROP USER $user;
DROP TABLE t_mt;
DROP TABLE t_mem;
"
