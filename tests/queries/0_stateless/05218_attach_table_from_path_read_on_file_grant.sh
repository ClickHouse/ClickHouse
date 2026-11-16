#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database
# no-object-storage: the query attaches a directory on the local disk, and there the table data lives on
# object storage.
# no-replicated-database: a Replicated database enqueues the query before the path is resolved against
# `user_files`, and the DDL worker then rejects the path as outside of `user_files`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ATTACH TABLE ... FROM '<dir>'` reads a directory in `user_files` as the data of the new table and moves it to
# the data path of the table, so it needs `READ ON FILE` like the `file` function and `WRITE ON FILE` like
# renaming files after processing. Without that check `CREATE TABLE` on a table of their own is enough for a
# user to read and consume data that somebody else staged in `user_files`.

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
staged_root="${CLICKHOUSE_TEST_UNIQUE_NAME}_staged"
staged_table_dir="${staged_root}/data/staging/protected"

# Stage a MergeTree table directory in `user_files` with clickhouse-local, as an admin or a migration
# workflow would do. An Ordinary database keeps the table directory at a known place.
${CLICKHOUSE_LOCAL} --path "${USER_FILES_PATH}/${staged_root}" --allow_deprecated_database_ordinary=1 --query "
    CREATE DATABASE staging ENGINE = Ordinary;
    CREATE TABLE staging.protected (id UInt64, secret UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO staging.protected VALUES (1, 424242);
"

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS stolen;
DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.stolen TO $user;
GRANT TABLE ENGINE ON MergeTree TO $user;
EOF

attach_query="ATTACH TABLE stolen FROM '${staged_table_dir}/' (id UInt64, secret UInt64) ENGINE = MergeTree ORDER BY id"

# The query is denied before it touches the directory: with no grant on the FILE source, and with `READ` alone.
check_denied_and_untouched()
{
    if ${CLICKHOUSE_CLIENT} --user "$user" --query "$attach_query" 2>&1 | grep -qF "$1"; then
        echo "ACCESS_DENIED: $1"
    else
        echo "UNEXPECTED: the attach was not denied with: $1"
    fi
    ${CLICKHOUSE_CLIENT} --query "SELECT 'tables named stolen:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'stolen'"
    if [ -d "${USER_FILES_PATH}/${staged_table_dir}" ]; then
        echo "staged directory is still in place"
    else
        echo "UNEXPECTED: the staged directory is gone"
    fi
}
check_denied_and_untouched "necessary to have the grant READ, WRITE ON FILE"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO $user"
check_denied_and_untouched "Missing permissions: WRITE ON FILE"

# With both grants the same query attaches the data.
${CLICKHOUSE_CLIENT} --query "GRANT WRITE ON FILE TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "$attach_query"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT secret FROM stolen"

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE stolen;
DROP USER $user;
EOF
rm -rf "${USER_FILES_PATH:?}/${staged_root}"
