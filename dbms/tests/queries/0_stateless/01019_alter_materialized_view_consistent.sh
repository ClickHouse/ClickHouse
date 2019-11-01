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

CREATE MATERIALIZED VIEW mv(test UInt8) Engine = MergeTree() ORDER BY test AS SELECT v == 1 as test FROM src_a;
EOF

INSERT[0]="INSERT INTO TABLE src_a VALUES (1);"
INSERT[1]="INSERT INTO TABLE src_b VALUES (2);"

for i in $(seq 1 100); do
  $CLICKHOUSE_CLIENT -q "${INSERT[$RANDOM % 2]}" 2>/dev/null &
done

seq 1 100 | sed -r -e "s/.+/ALTER TABLE mv MODIFY QUERY SELECT v == 1 as test FROM src_a;/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &
seq 1 100 | sed -r -e "s/.+/ALTER TABLE mv MODIFY QUERY SELECT v == 2 as test FROM src_b;/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &

wait

$CLICKHOUSE_CLIENT -q "SELECT 'inserts', count() > 0 FROM mv WHERE test == 1;"
$CLICKHOUSE_CLIENT -q "SELECT 'inconsistencies', count() FROM mv WHERE test == 0;"
