#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Depends on Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# URL-backed Parquet with a row policy on a DEFAULT column missing from the file must evaluate the
# policy after AddingDefaultsTransform (same as File / object storage), not inside the reader on
# type defaults.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05030_url_rp_default_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"
PARQUET="${DATA_DIR}/rp.parquet"
PARQUET_URL="file://${PARQUET}"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${PARQUET}', Parquet)
    SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

"${LOCAL[@]}" --query "
CREATE TABLE t_url_rp
(
    k UInt64,
    a UInt64,
    s String,
    d UInt64 DEFAULT a * 2
)
ENGINE = URL('${PARQUET_URL}', Parquet);

CREATE ROW POLICY pol_url_d ON t_url_rp USING d != 0 TO ALL;

SELECT count() FROM t_url_rp;
SELECT k, s FROM t_url_rp ORDER BY k LIMIT 3;
SELECT k, d FROM t_url_rp ORDER BY k LIMIT 3;

DROP ROW POLICY pol_url_d ON t_url_rp;
DROP TABLE t_url_rp;
"
