#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

rm -f "${CLICKHOUSE_TMP}/database.sqlite3" "${CLICKHOUSE_TMP}/test.json"

sqlite3 "${CLICKHOUSE_TMP}/database.sqlite3" 'CREATE TABLE _kv(k TEXT, v TEXT, PRIMARY KEY (k));'
echo '{"max_ts":123456}' > "${CLICKHOUSE_TMP}/test.json"

${CLICKHOUSE_LOCAL} --query="
CREATE DATABASE tmpdb ENGINE = SQLite('${CLICKHOUSE_TMP}/database.sqlite3');
INSERT INTO tmpdb._kv VALUES ('a', 'b');
SELECT 'max_ts' AS k, CAST(max_ts, 'String') AS v FROM file('${CLICKHOUSE_TMP}/test.json');
INSERT INTO tmpdb._kv SELECT 'max_ts' AS k, CAST(max_ts, 'String') AS v from file('${CLICKHOUSE_TMP}/test.json');
SELECT * FROM tmpdb._kv ORDER BY ALL;
"

rm "${CLICKHOUSE_TMP}/database.sqlite3" "${CLICKHOUSE_TMP}/test.json"
