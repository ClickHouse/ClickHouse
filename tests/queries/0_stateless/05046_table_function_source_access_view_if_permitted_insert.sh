#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_05046_insert"
base_url="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query="
insert_url="${base_url}INSERT%20INTO%20${CLICKHOUSE_DATABASE}.t_sink%20FORMAT%20CSV"
select_url="${base_url}SELECT%20%27from-url%27"

as_user()
{
    $CLICKHOUSE_CLIENT --user "$user_name" -q "$1" 2>&1 | grep -o "ACCESS_DENIED" | uniq
}

# The user cannot read `t_secret`, so `viewIfPermitted` takes the `ELSE` branch.
fallback()
{
    echo "viewIfPermitted(SELECT marker FROM ${CLICKHOUSE_DATABASE}.t_secret ELSE url('$1', 'CSV', 'marker String'))"
}

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_secret;
DROP TABLE IF EXISTS t_sink;
DROP USER IF EXISTS $user_name;
CREATE TABLE t_secret (marker String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_sink (marker String) ENGINE = MergeTree ORDER BY tuple();
CREATE USER $user_name NOT IDENTIFIED;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_name;
GRANT READ ON URL TO $user_name;
"

echo "=== READ ON URL: insert into url ==="
as_user "INSERT INTO FUNCTION url('$insert_url', 'CSV', 'marker String') SELECT 'direct'"
echo "=== READ ON URL: insert through viewIfPermitted ==="
as_user "INSERT INTO FUNCTION $(fallback "$insert_url") SELECT 'denied'"
echo "=== READ ON URL: select through viewIfPermitted ==="
$CLICKHOUSE_CLIENT --user "$user_name" -q "SELECT marker FROM $(fallback "$select_url")" 2>&1
echo "=== WRITE ON URL: insert through viewIfPermitted ==="
$CLICKHOUSE_CLIENT -q "GRANT WRITE ON URL TO $user_name"
$CLICKHOUSE_CLIENT --user "$user_name" -q "INSERT INTO FUNCTION $(fallback "$insert_url") SELECT 'allowed'" 2>&1
echo "=== rows in the sink ==="
$CLICKHOUSE_CLIENT -q "SELECT marker, count() FROM t_sink GROUP BY marker ORDER BY marker"

$CLICKHOUSE_CLIENT -q "
DROP TABLE t_secret;
DROP TABLE t_sink;
DROP USER $user_name;
"
