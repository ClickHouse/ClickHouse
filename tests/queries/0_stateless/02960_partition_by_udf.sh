#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function_name="$CLICKHOUSE_DATABASE"_02960_f1
$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $function_name;
CREATE FUNCTION $function_name AS (x) -> x;

CREATE TABLE hit
(
  UserID UInt32,
  URL String,
  EventTime DateTime
)
ENGINE = MergeTree
partition by $function_name(URL)
ORDER BY (EventTime);

INSERT INTO hit SELECT * FROM generateRandom() LIMIT 10;
SELECT count() FROM hit;

DROP TABLE hit;
"
