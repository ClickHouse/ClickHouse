#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TABLE_PATH="$USER_FILES_PATH/$CLICKHOUSE_TEST_UNIQUE_NAME"
mkdir -p "$TABLE_PATH"
trap 'rm -rf "$TABLE_PATH"' EXIT

$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE manifest_predicates (id Int32, part Int32)
ENGINE=IcebergLocal('$TABLE_PATH') PARTITION BY part;
INSERT INTO manifest_predicates SETTINGS allow_insert_into_iceberg=1 VALUES (1,0),(2,1),(3,2),(4,3),(5,1);
SELECT arraySort(groupArray(id)) FROM manifest_predicates WHERE part IN (0,2);
SELECT arraySort(groupArray(id)) FROM manifest_predicates WHERE part IN (1,3);
SELECT arraySort(groupArray(id)) FROM manifest_predicates WHERE (part=0 OR part=1) AND NOT (part=0 AND id=1);
SELECT arraySort(groupArray(id)) FROM manifest_predicates WHERE (part=0 OR part=1) AND NOT (part=0 AND id=9);
SELECT arraySort(groupArray(id)) FROM manifest_predicates WHERE part=1 AND (id=2 OR id=5);
SELECT arraySort(groupArray(id)) FROM manifest_predicates WHERE part=1 AND (id=2 OR id=9);
DROP TABLE manifest_predicates;
EOF
