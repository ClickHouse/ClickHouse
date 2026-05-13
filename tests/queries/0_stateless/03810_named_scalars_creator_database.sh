#!/usr/bin/env bash
# Tags: no-parallel
#
# Verify CREATE-time normalization rewrites unqualified identifiers with the
# user's current database in the persisted definition. The runtime "refresh
# re-resolves correctly after restart" property is covered by an integration
# test (test_creator_database_normalization in test_shared_named_scalars_cluster).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_LOCAL="$CLICKHOUSE_LOCAL --allow_experimental_named_scalars=1"

TMP_DIR="${CLICKHOUSE_TMP}/named_scalars_db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

# CREATE inside `mydb` so the session's currentDatabase is `mydb`. The scalar
# references `t` unqualified; CREATE-time normalization must rewrite it to
# `mydb.t` in the persisted .sql.
${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --multiquery --query "
CREATE DATABASE mydb;
CREATE TABLE mydb.t (x UInt64) ENGINE=MergeTree() ORDER BY x;
INSERT INTO mydb.t VALUES (10), (20), (30);
" 2>&1

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --database=mydb --query "
CREATE NAMED SCALAR cv_db REFRESH EVERY 36500 DAYS AS (SELECT sum(x) FROM t);
" 2>&1

# Sync CREATE eval already proves the value resolves to 60.
${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "SELECT getNamedScalar('cv_db')"

# Persisted .sql must carry the qualified identifier.
SQL_FILE="$(ls ${TMP_DIR%/}/named_scalars/named_scalar_cv_db.sql 2>/dev/null | head -1)"
if [ -n "$SQL_FILE" ] && [ -f "$SQL_FILE" ]; then
    grep -q 'FROM mydb.t' "$SQL_FILE" && echo "definition_is_qualified=1" || echo "definition_is_qualified=0"
else
    echo "sql_file_missing"
fi

rm -rf "$TMP_DIR"
