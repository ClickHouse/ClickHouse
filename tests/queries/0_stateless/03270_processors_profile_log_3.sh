#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "
  CREATE TABLE t
  (
      a UInt32,
      b UInt32
  )
  ENGINE = MergeTree
  ORDER BY (a, b);

  INSERT INTO t SELECT number, number FROM numbers(1000);
"

query_id="03270_processors_profile_log_3_$RANDOM"

$CLICKHOUSE_CLIENT --query_id="$query_id" -q "
  SET log_processors_profiles = 1;

  WITH
    t0 AS
    (
        SELECT *
        FROM numbers(1000)
    ),
    t1 AS
    (
        SELECT number * 3 AS b
        FROM t0
    )
  SELECT b * 3
  FROM t
  WHERE a IN (t1)
  FORMAT Null;
"

$CLICKHOUSE_CLIENT --query_id="$query_id" -q "
  SYSTEM FLUSH LOGS processors_profile_log;

  SELECT sum(elapsed_us) > 0
  FROM system.processors_profile_log
  WHERE event_date >= yesterday() AND query_id = '$query_id' AND name = 'CreatingSetsTransform';
"

#####################################################################

$CLICKHOUSE_CLIENT -q "
  CREATE TABLE t1
  (
    st FixedString(54)
  )
  ENGINE = MergeTree
  ORDER BY tuple();

  INSERT INTO t1 VALUES
  ('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ'),
  ('\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'),
  ('IIIIIIIIII\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0');
"

query_id="03270_processors_profile_log_3_$RANDOM"

$CLICKHOUSE_CLIENT --query_id="$query_id" -q "
  SET log_processors_profiles = 1;
  SET max_threads=2; -- no merging when max_threads=1

  WITH
    (
      SELECT groupConcat(',')(st)
      FROM t1
      ORDER BY ALL
    ) AS a,
    (
      SELECT groupConcat(',')(CAST(st, 'String'))
      FROM t1
      ORDER BY ALL
    ) AS b
  SELECT a = b
  FORMAT Null;
"

$CLICKHOUSE_CLIENT --query_id="$query_id" -q "
  SYSTEM FLUSH LOGS processors_profile_log;

  SELECT sum(elapsed_us) > 0
  FROM system.processors_profile_log
  WHERE event_date >= yesterday() AND query_id = '$query_id' AND name = 'MergingSortedTransform';
"

