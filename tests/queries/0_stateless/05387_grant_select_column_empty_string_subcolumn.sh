#!/usr/bin/env bash

# https://github.com/ClickHouse/ClickHouse/issues/122036
# `c2 != ''` is rewritten to read the `c2.size` subcolumn of a `String` column, which must be covered by `GRANT SELECT(c2)`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05387_${CLICKHOUSE_DATABASE}"
settings="SETTINGS optimize_empty_string_comparisons = 1, optimize_functions_to_subcolumns = 1"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE t1 (c1 String, c2 String) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t1 SELECT toString(number), if(number % 3 = 0, '', 'x') FROM numbers(100);
CREATE VIEW v1 (c1 String, c2 String) AS SELECT c1, c2 FROM t1;
CREATE USER $user;
GRANT SELECT(c1, c2) ON $CLICKHOUSE_DATABASE.t1 TO $user;
GRANT SELECT(c1, c2) ON $CLICKHOUSE_DATABASE.v1 TO $user;
CREATE VIEW v2 DEFINER = $user SQL SECURITY DEFINER AS SELECT count() AS n FROM v1 WHERE c2 != '';
"

$CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM $CLICKHOUSE_DATABASE.t1 WHERE c2 != '' $settings"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM $CLICKHOUSE_DATABASE.v1 WHERE c2 != '' $settings"
$CLICKHOUSE_CLIENT -q "SELECT n FROM v2 $settings"

$CLICKHOUSE_CLIENT -q "DROP VIEW v2; DROP USER $user;"
