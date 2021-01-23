#!/usr/bin/env bash

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --multiquery --query "
DROP TABLE IF EXISTS items;

CREATE TABLE items (
  time DateTime,
  group_id UInt16,
  value UInt32()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (group_id, time);
"

$CLICKHOUSE_CLIENT --multiquery --query "
SELECT
  group_id,
  groupArray(time)[1] as time,
  groupArray(value)                       ,
FROM (
  SELECT * FROM items ORDER BY time desc LIMIT 100 BY group_id
)
GROUP BY group_id;
" 2>&1 | grep -oF 'Syntax error'

$CLICKHOUSE_CLIENT --multiquery --query "
SELECT
  group_id,
  groupArray(time)[1] as time,
  groupArray(value)                       ,
FRO (
  SELECT * FROM items ORDER BY time desc LIMIT 100 BY group_id
)
GROUP BY group_id;
" 2>&1 | grep -oF 'Code: 36.'

$CLICKHOUSE_CLIENT --multiquery --query "
DROP TABLE IF EXISTS items;
"
