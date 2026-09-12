#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The storage-engine read path (SQLiteStatementReader in its native read mode) must accept explicitly
# declared LowCardinality(...) columns: ExternalResultDescription does not unwrap low-cardinality
# wrappers, so such columns are read through the text path, like the rest of the SQLite code, which
# treats LowCardinality transparently.

DB_PATH="${CLICKHOUSE_TMP}/04643_sqlite_lc.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "
CREATE TABLE t (s TEXT, f REAL, i INTEGER);
INSERT INTO t VALUES ('abc', 1.5, 7), ('abc', NULL, 8), ('xyz', -2.25, 9);
"

${CLICKHOUSE_LOCAL} --query "
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE ch (s LowCardinality(String), f LowCardinality(Nullable(Float64)), i LowCardinality(Int64)) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'LowCardinality columns over the SQLite storage engine:';
SELECT * FROM ch ORDER BY i;

SELECT 'The read keeps the declared LowCardinality types:';
SELECT toTypeName(s), toTypeName(f), toTypeName(i) FROM ch LIMIT 1;
"

rm -f "${DB_PATH}"
