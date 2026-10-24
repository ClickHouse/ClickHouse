#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# A tuple element of an Iceberg table must be readable from data files written before a schema change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_tuple_element"
rm -rf "${ICEBERG_PATH}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_ice (id Int64, s Tuple(a Int64, b Nullable(Int64), c Tuple(x Int64)), arr Array(Tuple(y Int64)))
        ENGINE = IcebergLocal('${ICEBERG_PATH}');
    INSERT INTO t_ice VALUES (1, (10, 100, (1000)), [(7)]), (2, (20, NULL, (2000)), [(8), (9)]);
    ALTER TABLE t_ice ADD COLUMN extra Nullable(Int64);
    INSERT INTO t_ice VALUES (3, (30, 300, (3000)), [], 5);
    SELECT id, s.a, s.b, s.c.x, arr.y FROM t_ice ORDER BY id;
    SELECT id FROM t_ice WHERE s.a = 20;
    SELECT id, s.a FROM icebergLocal('${ICEBERG_PATH}') ORDER BY id;
    ALTER TABLE t_ice RENAME COLUMN s TO r;
    SELECT id, r.a FROM t_ice ORDER BY id;
    DROP TABLE t_ice;
"

rm -rf "${ICEBERG_PATH}"
