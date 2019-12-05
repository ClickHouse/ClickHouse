#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Smoke tests for freeze partition.

function test_freeze_date_partition() {
  local suffix=$1

  $CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS t_01043_freeze;
CREATE TABLE t_01043_freeze
(
  d Date
) ENGINE = MergeTree(d, d, 8192);

INSERT INTO
  t_01043_freeze
VALUES
  (toDate('2019-01-01')), (toDate('2019-02-01')), (toDate('2020-01-01'));
EOF

  $CLICKHOUSE_CLIENT --query "FREEZE TABLE t_01043_freeze $suffix" | grep -qP "\d+"
  $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE table = 't_01043_freeze' and is_frozen"
}

function test_freeze_custom_partition() {
  local suffix=$1
  local exception=$2

  $CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS t_01043_freeze;
CREATE TABLE t_01043_freeze
(
  d Date
) ENGINE = MergeTree()
  ORDER BY d
  PARTITION BY d;

INSERT INTO
  t_01043_freeze
VALUES
  (toDate('2019-01-01')), (toDate('2019-02-01')), (toDate('2020-01-01'));
EOF

  if [[ ! -z $exception ]]; then
    $CLICKHOUSE_CLIENT --query "FREEZE TABLE t_01043_freeze $suffix" --format Null 2>&1 |
      grep -q "Cannot parse date" && echo "OK" || echo "FAIL"

    return
  fi

  $CLICKHOUSE_CLIENT --query "FREEZE TABLE t_01043_freeze $suffix" | grep -qP "\d+"
  $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE table = 't_01043_freeze' and is_frozen"
}

test_freeze_date_partition ""
test_freeze_date_partition "PARTITION 201901"
test_freeze_date_partition "PARTITION '2019'"
# test_freeze_date_partition "PARTITION ID '201901'" doesn't work in master either

echo "---custom_partition---"

test_freeze_custom_partition ""
test_freeze_custom_partition "PARTITION 201901"
test_freeze_custom_partition "PARTITION '2019'" "Cannot parse date"
test_freeze_custom_partition "PARTITION '2020-01-01'"
test_freeze_custom_partition "PARTITION ID '20190101'"
