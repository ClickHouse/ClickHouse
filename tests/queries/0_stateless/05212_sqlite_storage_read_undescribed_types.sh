#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_undescribed_types.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# The storage-engine read path (`SQLiteStatementReader` in its native read mode) decodes a column natively
# only when `ExternalResultDescription` has a value type for it. Types it cannot describe - `Variant`,
# `Tuple`, `Map`, `IPv4`, ... - used to reach `ExternalResultDescription::init` and fail the first
# `SELECT` with `UNKNOWN_TYPE`, although `CREATE TABLE` had accepted the column and `INSERT` had written
# it (as its text serialization). Such columns are read through the text path instead. (`Dynamic` and
# `JSON` never get that far: the storage does not support columns with a dynamic structure and rejects
# them when the table is created.)
sqlite3 "$DB_PATH" "CREATE TABLE t (id INTEGER PRIMARY KEY, v ANY, tp TEXT, m TEXT, ip TEXT, nip TEXT);"

${CLICKHOUSE_LOCAL} --multiquery --query="
CREATE TABLE ext_dynamic (id Int64, d Dynamic) ENGINE = SQLite('${DB_PATH}', 't'); -- { serverError ILLEGAL_COLUMN }

CREATE TABLE ext (
    id Int64,
    v Variant(String, UInt64),
    tp Tuple(a Int64, b String),
    m Map(String, Int64),
    ip IPv4,
    nip Nullable(IPv6)
) ENGINE = SQLite('${DB_PATH}', 't');

INSERT INTO ext VALUES
    (1, 7, (1, 'one'), map('k', 1), '10.0.0.1', NULL),
    (2, 'x', (2, 'two'), map('k', 2, 'l', 3), '192.168.1.2', '2001:db8::1');

SELECT 'rows read back through the storage engine';
SELECT * FROM ext ORDER BY id;

SELECT 'declared types are kept';
SELECT toTypeName(v), toTypeName(tp), toTypeName(m), toTypeName(ip), toTypeName(nip) FROM ext LIMIT 1;

SELECT 'inner types of Variant';
SELECT id, variantType(v) FROM ext ORDER BY id;
"

echo 'the same values through the sqlite table function'
${CLICKHOUSE_LOCAL} --query="
SELECT id, v, tp FROM sqlite('${DB_PATH}', 't') ORDER BY id;
"
