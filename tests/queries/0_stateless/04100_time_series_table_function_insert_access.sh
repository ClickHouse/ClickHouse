#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
table="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --enable_time_series_table 1 -q "
DROP TABLE IF EXISTS $table;
DROP USER IF EXISTS $user;
CREATE TABLE $table ENGINE = TimeSeries;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT SHOW TABLES, SHOW COLUMNS, SELECT ON $CLICKHOUSE_DATABASE.$table TO $user;
"

$CLICKHOUSE_CLIENT --user $user -q "INSERT INTO TABLE FUNCTION timeSeriesSamples('$CLICKHOUSE_DATABASE', '$table') SELECT * FROM timeSeriesSamples('$CLICKHOUSE_DATABASE', '$table') WHERE false" 2>&1 | grep -o "ACCESS_DENIED" | head -1

$CLICKHOUSE_CLIENT -q "GRANT INSERT ON $CLICKHOUSE_DATABASE.$table TO $user"
$CLICKHOUSE_CLIENT --user $user -q "INSERT INTO TABLE FUNCTION timeSeriesSamples('$CLICKHOUSE_DATABASE', '$table') SELECT * FROM timeSeriesSamples('$CLICKHOUSE_DATABASE', '$table') WHERE false" && echo OK

$CLICKHOUSE_CLIENT -q "DROP USER $user; DROP TABLE $table;"
