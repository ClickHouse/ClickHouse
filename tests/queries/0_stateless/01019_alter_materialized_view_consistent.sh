#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS src_a;
DROP TABLE IF EXISTS src_b;

DROP TABLE IF EXISTS mv;

CREATE TABLE src_a(v UInt64) ENGINE = Null;
CREATE TABLE src_b(v UInt64) ENGINE = Null;

CREATE MATERIALIZED VIEW mv(test UInt8, case UInt8)
Engine = MergeTree()
ORDER BY test AS
SELECT v == 1 as test, v as case FROM src_a;
EOF

# The purpose of this test is to ensure that MV query A is always used against source table A. Same for query/table B.
# Also, helps detect data races.

function insert_thread() {
<<<<<<< HEAD
  # Always wait for all background INSERTs to finish to catch stuck queries.
  trap 'wait; exit;' INT

  INSERT[0]="INSERT INTO TABLE src_a VALUES (1);"
  INSERT[1]="INSERT INTO TABLE src_b VALUES (2);"

  while true; do
    # trigger 100 concurrent inserts at a time
    for i in $(seq 1 100); do
      # ignore `Possible deadlock avoided. Client should retry`
      $CLICKHOUSE_CLIENT -q "${INSERT[$RANDOM % 2]}" 2>/dev/null &
=======
    # Always wait for all background INSERTs to finish to catch stuck queries.
    trap 'wait; exit;' INT

    INSERT[0]="INSERT INTO TABLE src_a VALUES (1);"
    INSERT[1]="INSERT INTO TABLE src_b VALUES (2);"

    while true; do
        # trigger 50 concurrent inserts at a time
        for i in {0..50}; do
            # ignore `Possible deadlock avoided. Client should retry`
            $CLICKHOUSE_CLIENT -q "${INSERT[$RANDOM % 2]}" 2>/dev/null &
        done
        wait

        is_done=$($CLICKHOUSE_CLIENT -q "SELECT countIf(case = 1) > 0 AND countIf(case = 2) > 0 FROM mv;")

        if [ "$is_done" -eq "1" ]; then
            break
        fi
>>>>>>> upstream/master
    done

    wait
  done
}

function alter_thread() {
  trap 'exit' INT

  ALTER[0]="ALTER TABLE mv MODIFY QUERY SELECT v == 1 as test, v as case FROM src_a;"
  ALTER[1]="ALTER TABLE mv MODIFY QUERY SELECT v == 2 as test, v as case FROM src_b;"

<<<<<<< HEAD
  while true; do
    $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 \
      -q "${ALTER[$RANDOM % 2]}"
    sleep "0.0$RANDOM"
  done
=======
    while true; do
        $CLICKHOUSE_CLIENT --allow_experimental_alter_materialized_view_structure=1 \
        -q "${ALTER[$RANDOM % 2]}"
        sleep "0.0$RANDOM"

        is_done=$($CLICKHOUSE_CLIENT -q "SELECT countIf(case = 1) > 0 AND countIf(case = 2) > 0 FROM mv;")

        if [ "$is_done" -eq "1" ]; then
            break
        fi
    done
>>>>>>> upstream/master
}

insert_thread &
insert_thread_pid=$!

<<<<<<< HEAD
alter_thread &
alter_thread_pid=$!

while true; do
  is_done=$($CLICKHOUSE_CLIENT -q "SELECT countIf(case = 1) > 0 AND countIf(case = 2) > 0 FROM mv;")

  if [ "$is_done" -eq "1" ]; then
    break
  fi
done

kill -INT $insert_thread_pid
kill -INT $alter_thread_pid
=======
# finishes much faster with all builds, except debug with coverage
timeout 120 bash -c insert_thread &
timeout 120 bash -c alter_thread &
>>>>>>> upstream/master

wait

$CLICKHOUSE_CLIENT -q "SELECT countIf(case = 1) > 0 AND countIf(case = 2) > 0 FROM mv LIMIT 1;"
$CLICKHOUSE_CLIENT -q "SELECT 'inconsistencies', count() FROM mv WHERE test == 0;"
