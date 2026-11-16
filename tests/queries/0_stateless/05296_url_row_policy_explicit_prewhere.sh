#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Explicit PREWHERE on a `URL` table with a row policy and a `DEFAULT` column computed from the policy input.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
CREATE TABLE t (k UInt64, a UInt64, s String, d UInt64 DEFAULT a * 2)
ENGINE = URL('http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+k,+number+%25+10+AS+a,+concat(%27val_%27,+toString(number))+AS+s+FROM+numbers(1000)+FORMAT+Parquet', Parquet);
CREATE ROW POLICY p ON t USING a != 0 TO ALL;
SELECT k, d FROM t PREWHERE s != 'val_2' ORDER BY k LIMIT 3;
SELECT count(), sum(d) FROM t PREWHERE s != 'val_2';
SELECT countIf(explain LIKE '% a UInt64') FROM (EXPLAIN header = 1 SELECT k, s FROM t PREWHERE s != 'val_2');
DROP ROW POLICY p ON t;
DROP TABLE t;
"
