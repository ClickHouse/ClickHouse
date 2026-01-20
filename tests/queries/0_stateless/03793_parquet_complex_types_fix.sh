#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="default.tm1"
PARQUET_FILE="${CLICKHOUSE_TMP}/03793_parquet_complex_types_fix.parquet"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS ${TABLE};

CREATE TABLE ${TABLE}
(
    a Map(Tuple(a String, b String), String)
)
ENGINE = MergeTree
ORDER BY tuple();
"

${CLICKHOUSE_CLIENT} --query="
INSERT INTO ${TABLE}
SELECT mapFromArrays(
    [tuple('kA1','kB1'), tuple('kA2','kB2')],
    ['v1','v2']
);
"

${CLICKHOUSE_CLIENT} --query="
SELECT *
FROM ${TABLE}
INTO OUTFILE '${PARQUET_FILE}'
FORMAT Parquet
"

${CLICKHOUSE_CLIENT} --query="
SELECT *
FROM file('${PARQUET_FILE}', Parquet);
"
