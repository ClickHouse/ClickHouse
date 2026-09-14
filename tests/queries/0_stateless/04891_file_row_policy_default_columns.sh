#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Depends on Parquet
# https://github.com/ClickHouse/ClickHouse/issues/114616
# Row policy / PREWHERE over File tables must compute DEFAULT columns from real
# dependency columns, not prune those inputs away before AddingDefaultsTransform.
# Per-database path under a private --path so parallel flaky-check copies do not
# truncate each other's Parquet files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04891_file_row_policy_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --enable_json_type=1)

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"
FILE="${CLICKHOUSE_DATABASE}_04891_rp.parquet"

"${LOCAL[@]}" --query "
INSERT INTO FUNCTION file('${DATA_DIR}/${FILE}', Parquet)
SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
FROM numbers(1000)
SETTINGS engine_file_truncate_on_insert = 1;
"

"${LOCAL[@]}" --multiquery <<EOF
-- Case 1: row policy on a DEFAULT column missing from the file.
DROP TABLE IF EXISTS t_rp_j;
CREATE TABLE t_rp_j
(
    k UInt64,
    a UInt64,
    s String,
    j JSON DEFAULT toJSONString(map('user', map('name', concat('u', toString(a)))))
)
ENGINE = File(Parquet, '${DATA_DIR}/${FILE}');

CREATE ROW POLICY pol_j ON t_rp_j USING j.user.name != 'u0' TO ALL;

SELECT count() FROM t_rp_j;
SELECT k, s FROM t_rp_j ORDER BY k LIMIT 3;

DROP ROW POLICY pol_j ON t_rp_j;
DROP TABLE t_rp_j;

-- Case 2: policy on a file column; SELECT a DEFAULT column with PREWHERE.
DROP TABLE IF EXISTS t_rp_d;
CREATE TABLE t_rp_d
(
    k UInt64,
    a UInt64,
    s String,
    d UInt64 DEFAULT a * 2
)
ENGINE = File(Parquet, '${DATA_DIR}/${FILE}');

CREATE ROW POLICY pol_d ON t_rp_d USING a != 0 TO ALL;

SELECT k, d FROM t_rp_d PREWHERE s != 'val_2' ORDER BY k LIMIT 3;
SELECT k, d FROM t_rp_d ORDER BY k LIMIT 3;

DROP ROW POLICY pol_d ON t_rp_d;
DROP TABLE t_rp_d;

-- Case 3: row policy on a tuple-element subcolumn of a DEFAULT parent missing from the file.
-- The supports_tuple_elements path can keep \`t.x\` as a real subcolumn name; DEFAULT checks
-- must resolve through the storage parent \`t\`.
DROP TABLE IF EXISTS t_rp_t;
CREATE TABLE t_rp_t
(
    k UInt64,
    a UInt64,
    s String,
    t Tuple(x UInt64, y String) DEFAULT (a * 2, s)
)
ENGINE = File(Parquet, '${DATA_DIR}/${FILE}');

CREATE ROW POLICY pol_t ON t_rp_t USING t.x != 0 TO ALL;

SELECT count() FROM t_rp_t;
SELECT k, s FROM t_rp_t ORDER BY k LIMIT 3;
SELECT t.x FROM t_rp_t ORDER BY k LIMIT 3;

DROP ROW POLICY pol_t ON t_rp_t;
DROP TABLE t_rp_t;
EOF
