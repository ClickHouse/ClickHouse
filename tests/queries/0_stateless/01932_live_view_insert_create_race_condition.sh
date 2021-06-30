#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()" | tr '-' '_')

ITERATIONS=10

echo -e "DROP TABLE IF EXISTS mt_${UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null
echo -e "CREATE TABLE mt_${UUID} (i UInt64) ENGINE = MergeTree() PARTITION BY i ORDER BY i" | ${CLICKHOUSE_CLIENT} -n > /dev/null

for _ in $(seq 1 $ITERATIONS)
do
	echo -e "INSERT INTO mt_${UUID} VALUES (1),(2),(3),(4)" | ${CLICKHOUSE_CLIENT} -n > /dev/null &
	echo -e "DROP TABLE IF EXISTS lv_insert_create_race_condition_${UUID}" | ${CLICKHOUSE_CLIENT} -n > /dev/null &
	echo -e "CREATE LIVE VIEW IF NOT EXISTS lv_insert_create_race_condition_${UUID} AS SELECT * FROM mt_${UUID}" | ${CLICKHOUSE_CLIENT} --allow_experimental_live_view 1 -n > /dev/null &
done

wait
