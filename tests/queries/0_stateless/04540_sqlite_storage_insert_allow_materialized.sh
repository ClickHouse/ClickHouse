#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=$(mktemp "$CLICKHOUSE_TMP/sqlite_insert_allow_materialized_XXXXXX.sqlite")
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" 'CREATE TABLE remote_table(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);'

DDL="CREATE TABLE local_table ENGINE = SQLite('$DB_PATH', 'remote_table')"

echo 'An explicitly inserted generated column is rejected by SQLite:'
${CLICKHOUSE_LOCAL} --multiquery --query "
    SET insert_allow_materialized_columns = 1;
    ${DDL};
    INSERT INTO local_table (a, b) VALUES (1, 100);
" 2>&1 | grep -oF -m1 'cannot INSERT into generated column "b"'

echo 'The rejected insert wrote nothing:'
${CLICKHOUSE_LOCAL} --multiquery --query "${DDL}; SELECT count() FROM local_table;"

# The insert pipeline also contains an automatically added placeholder for generated column `b`. It must
# still be removed when the user only names base column `a`, so SQLite can compute the generated value.
echo 'An insert naming only base columns still succeeds:'
${CLICKHOUSE_LOCAL} --multiquery --query "
    SET insert_allow_materialized_columns = 1;
    ${DDL};
    INSERT INTO local_table (a) VALUES (2);
    SELECT a, b FROM local_table FORMAT TSVWithNames;
"
