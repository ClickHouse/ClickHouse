#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Depends on Parquet
# https://github.com/ClickHouse/ClickHouse/issues/114616
# A DEFAULT expression can depend on a subcolumn (`d DEFAULT j.a`). The dependency must be
# resolved through its storage parent, otherwise the parent is never read and the DEFAULT
# fails with UNKNOWN_IDENTIFIER or is evaluated against the wrong inputs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05031_default_subcolumn_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --enable_json_type=1)

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"
JSON_FILE="${DATA_DIR}/${CLICKHOUSE_DATABASE}_05031_json.parquet"
TUPLE_FILE="${DATA_DIR}/${CLICKHOUSE_DATABASE}_05031_tuple.parquet"

"${LOCAL[@]}" --query "
INSERT INTO FUNCTION file('${JSON_FILE}', Parquet)
SELECT number AS k, concat('{\"a\":', toString(number), '}')::JSON AS j
FROM numbers(5)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('${TUPLE_FILE}', Parquet)
SELECT number AS k, tuple(number, number * 2)::Tuple(a UInt32, b UInt32) AS t
FROM numbers(5)
SETTINGS engine_file_truncate_on_insert = 1;
"

"${LOCAL[@]}" --multiquery <<EOF
-- Case 1: DEFAULT depending on a dynamic subcolumn of a JSON column stored in the file.
DROP TABLE IF EXISTS t_sub_j;
CREATE TABLE t_sub_j
(
    k UInt64,
    j JSON,
    d UInt64 DEFAULT toUInt64(j.a)
)
ENGINE = File(Parquet, '${JSON_FILE}');

SELECT k, d FROM t_sub_j ORDER BY k;
SELECT k, d FROM t_sub_j WHERE d = 3 ORDER BY k;
SELECT k, d FROM t_sub_j PREWHERE k > 2 ORDER BY k;

CREATE ROW POLICY pol_sub_j ON t_sub_j USING d % 2 = 1 TO ALL;

SELECT count() FROM t_sub_j;
SELECT k FROM t_sub_j ORDER BY k;
SELECT k, d FROM t_sub_j PREWHERE k < 4 ORDER BY k;

DROP ROW POLICY pol_sub_j ON t_sub_j;
DROP TABLE t_sub_j;

-- Case 2: DEFAULT depending on a tuple element of a column stored in the file.
DROP TABLE IF EXISTS t_sub_t;
CREATE TABLE t_sub_t
(
    k UInt64,
    t Tuple(a UInt32, b UInt32),
    d UInt32 DEFAULT t.a * 10
)
ENGINE = File(Parquet, '${TUPLE_FILE}');

SELECT k, d FROM t_sub_t ORDER BY k;

CREATE ROW POLICY pol_sub_t ON t_sub_t USING d >= 20 TO ALL;

SELECT count() FROM t_sub_t;
SELECT k FROM t_sub_t ORDER BY k;

DROP ROW POLICY pol_sub_t ON t_sub_t;
DROP TABLE t_sub_t;
EOF
