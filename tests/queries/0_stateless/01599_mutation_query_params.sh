#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
  id UUID,
  date_time DateTime,
  x UInt32,
  y UInt32
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date_time)
ORDER BY (date_time);

INSERT INTO test (x, y) VALUES (2, 1);
"

$CLICKHOUSE_CLIENT --query "SELECT x, y FROM test"

$CLICKHOUSE_CLIENT --mutations_sync 1 --param_x 1 --param_y 1 --query "
ALTER TABLE test
UPDATE x = {x:UInt32}
WHERE y = {y:UInt32};
"

$CLICKHOUSE_CLIENT --query "SELECT x, y FROM test"
$CLICKHOUSE_CLIENT --query "DROP TABLE test"
