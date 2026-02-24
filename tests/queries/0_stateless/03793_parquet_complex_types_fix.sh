#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="default.tm_$RANDOM_$RANDOM"
PARQUET_FILE="${USER_FILES_PATH}/03793_parquet_complex_types_fix_$RANDOM_$RANDOM.parquet"

rm -rf "${PARQUET_FILE}"

${CLICKHOUSE_LOCAL} --query="
DROP TABLE IF EXISTS ${TABLE};

CREATE TABLE ${TABLE}
(
    a Map(Tuple(a String, b String), String)
)
ENGINE = Memory();

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
