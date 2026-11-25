#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=$(mktemp "$CLICKHOUSE_TMP/sqlite_tf_insert_allow_materialized_XXXXXX.sqlite")
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" 'CREATE TABLE remote_table(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED);'

# An INSERT into the `sqlite` table function records its explicit column list without an insertion table.
# A generated column named in that list must still reach SQLite and be rejected there, exactly like the
# named-table path, instead of being silently dropped from the outgoing INSERT.
echo 'An explicitly inserted generated column is rejected by SQLite:'
${CLICKHOUSE_LOCAL} --multiquery --query "
    SET insert_allow_materialized_columns = 1;
    INSERT INTO FUNCTION sqlite('$DB_PATH', 'remote_table') (a, b) VALUES (1, 100);
" 2>&1 | grep -oF -m1 'cannot INSERT into generated column "b"'

echo 'The rejected insert wrote nothing:'
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM sqlite('$DB_PATH', 'remote_table')"

# The insert pipeline also contains an automatically added placeholder for generated column `b`. It must
# still be removed when the user only names base column `a`, so SQLite can compute the generated value.
echo 'An insert naming only base columns still succeeds:'
${CLICKHOUSE_LOCAL} --multiquery --query "
    SET insert_allow_materialized_columns = 1;
    INSERT INTO FUNCTION sqlite('$DB_PATH', 'remote_table') (a) VALUES (2);
    SELECT a, b FROM sqlite('$DB_PATH', 'remote_table') FORMAT TSVWithNames;
"
