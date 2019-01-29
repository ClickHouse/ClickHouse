#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo "DROP TABLE IF EXISTS test.test" | ${CLICKHOUSE_CLIENT}
echo "CREATE TABLE test.test (ts DATETIME) ENGINE = MergeTree PARTITION BY toStartOfDay(ts) ORDER BY tuple()" | ${CLICKHOUSE_CLIENT}

for i in {1..500}; do echo "ALTER TABLE test.test ADD COLUMN c$i DOUBLE;"; done | ${CLICKHOUSE_CLIENT} -n

for i in {1..500}; do echo "ALTER TABLE test.test ADD COLUMN d DOUBLE" | ${CLICKHOUSE_CLIENT}; echo "ALTER TABLE test.test DROP COLUMN d" | ${CLICKHOUSE_CLIENT} -n; done &
for i in {1..500}; do echo "ALTER TABLE test.test ADD COLUMN e DOUBLE" | ${CLICKHOUSE_CLIENT}; echo "ALTER TABLE test.test DROP COLUMN e" | ${CLICKHOUSE_CLIENT} -n; done &

wait

echo "DROP TABLE test.test" | ${CLICKHOUSE_CLIENT}
