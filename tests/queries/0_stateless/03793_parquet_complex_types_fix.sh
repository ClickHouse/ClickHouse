#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="default.tm1"
PARQUET_FILE="${USER_FILES_PATH}/03793_parquet_complex_types_fix.parquet"

rm -rf "${PARQUET_FILE}"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS ${TABLE};

CREATE TABLE ${TABLE}
(
    a Map(Tuple(a String, b String), String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO ${TABLE}
SELECT mapFromArrays(
    [tuple('kA1','kB1'), tuple('kA2','kB2')],
    ['v1','v2']
);

SELECT *
FROM ${TABLE}
INTO OUTFILE '${PARQUET_FILE}'
FORMAT Parquet;

SELECT *
FROM file('${PARQUET_FILE}', Parquet);
"
