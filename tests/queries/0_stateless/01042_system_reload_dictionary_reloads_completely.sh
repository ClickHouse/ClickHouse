#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

# NOTE: dictionaries TTLs works with server timezone, so session_timeout cannot be used
$CLICKHOUSE_CLIENT --session_timezone '' --multiquery <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.table(x Int64, y Int64, insert_time DateTime) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (12, 102, now());

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict
(
  x Int64 DEFAULT -1,
  y Int64 DEFAULT -1,
  insert_time DateTime
)
PRIMARY KEY x
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table' DB '${CLICKHOUSE_DATABASE}' UPDATE_FIELD 'insert_time'))
LAYOUT(FLAT())
LIFETIME(1);
EOF

$CLICKHOUSE_CLIENT --query "SELECT '12 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"

$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (13, 103, now())"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (14, 104, now() - INTERVAL 1 DAY)"

while [ "$(${CLICKHOUSE_CLIENT} --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))")" = -1 ]
    do
        sleep 0.5
    done

$CLICKHOUSE_CLIENT --query "SELECT '13 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY '${CLICKHOUSE_DATABASE}.dict'"

$CLICKHOUSE_CLIENT --query "SELECT '12(r) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13(r) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14(r) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"
