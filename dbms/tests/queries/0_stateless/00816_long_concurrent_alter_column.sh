#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo "DROP TABLE IF EXISTS concurrent_alter_column" | ${CLICKHOUSE_CLIENT}
echo "CREATE TABLE concurrent_alter_column (ts DATETIME) ENGINE = MergeTree PARTITION BY toStartOfDay(ts) ORDER BY tuple()" | ${CLICKHOUSE_CLIENT}

for i in {1..500}; do echo "ALTER TABLE concurrent_alter_column ADD COLUMN c$i DOUBLE;"; done | ${CLICKHOUSE_CLIENT} -n

for i in {1..100}; do echo "ALTER TABLE concurrent_alter_column ADD COLUMN d DOUBLE" | ${CLICKHOUSE_CLIENT}; sleep `echo 0.0$RANDOM`; echo "ALTER TABLE concurrent_alter_column DROP COLUMN d" | ${CLICKHOUSE_CLIENT} -n; done &
for i in {1..100}; do echo "ALTER TABLE concurrent_alter_column ADD COLUMN e DOUBLE" | ${CLICKHOUSE_CLIENT}; sleep `echo 0.0$RANDOM`; echo "ALTER TABLE concurrent_alter_column DROP COLUMN e" | ${CLICKHOUSE_CLIENT} -n; done &
for i in {1..100}; do echo "ALTER TABLE concurrent_alter_column ADD COLUMN f DOUBLE" | ${CLICKHOUSE_CLIENT}; sleep `echo 0.0$RANDOM`; echo "ALTER TABLE concurrent_alter_column DROP COLUMN f" | ${CLICKHOUSE_CLIENT} -n; done &

wait

echo "DROP TABLE concurrent_alter_column" | ${CLICKHOUSE_CLIENT}

echo 'did not crash'
