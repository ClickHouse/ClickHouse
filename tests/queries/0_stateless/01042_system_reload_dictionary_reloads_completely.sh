#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
DROP DATABASE IF EXISTS dictdb_01042;
CREATE DATABASE dictdb_01042;
CREATE TABLE dictdb_01042.table(x Int64, y Int64, insert_time DateTime) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dictdb_01042.table VALUES (12, 102, now());

CREATE DICTIONARY dictdb_01042.dict
(
  x Int64 DEFAULT -1,
  y Int64 DEFAULT -1,
  insert_time DateTime
)
PRIMARY KEY x
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table' DB 'dictdb_01042' UPDATE_FIELD 'insert_time'))
LAYOUT(FLAT())
LIFETIME(1);
EOF

$CLICKHOUSE_CLIENT --query "SELECT '12 -> ', dictGetInt64('dictdb_01042.dict', 'y', toUInt64(12))"

$CLICKHOUSE_CLIENT --query "INSERT INTO dictdb_01042.table VALUES (13, 103, now())"
$CLICKHOUSE_CLIENT --query "INSERT INTO dictdb_01042.table VALUES (14, 104, now() - INTERVAL 1 DAY)"

while [ "$(${CLICKHOUSE_CLIENT} --query "SELECT dictGetInt64('dictdb_01042.dict', 'y', toUInt64(13))")" = -1 ]
    do
        sleep 0.5
    done

$CLICKHOUSE_CLIENT --query "SELECT '13 -> ', dictGetInt64('dictdb_01042.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 -> ', dictGetInt64('dictdb_01042.dict', 'y', toUInt64(14))"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY 'dictdb_01042.dict'"

$CLICKHOUSE_CLIENT --query "SELECT '12(r) -> ', dictGetInt64('dictdb_01042.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13(r) -> ', dictGetInt64('dictdb_01042.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14(r) -> ', dictGetInt64('dictdb_01042.dict', 'y', toUInt64(14))"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS dictdb_01042"
