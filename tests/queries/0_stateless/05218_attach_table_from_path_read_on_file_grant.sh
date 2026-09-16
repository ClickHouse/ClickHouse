#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database
# no-object-storage: the test copies the part files of a table on the local disk into `user_files`.
# no-replicated-database: a Replicated database enqueues the query before the path is resolved against
# `user_files`, and the DDL worker then rejects the path as outside of `user_files`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ATTACH TABLE ... FROM '<dir>'` adopts a directory in `user_files` as the data of the new table, so it needs
# `READ ON FILE`, the same grant as the `file` function. Without that check `CREATE TABLE` on a table of their
# own is enough for a user to read data that somebody else staged in `user_files`.

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
staged_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_staged"

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS protected;
DROP TABLE IF EXISTS stolen;
CREATE TABLE protected (id UInt64, secret UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO protected VALUES (1, 424242);
DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.stolen TO $user;
GRANT TABLE ENGINE ON MergeTree TO $user;
EOF

# Stage the data of `protected` in `user_files`, as an admin or a migration workflow would do.
data_dir=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'protected'")
cp -r "$data_dir" "${USER_FILES_PATH}/${staged_dir}"

attach_query="ATTACH TABLE stolen FROM '${staged_dir}/' (id UInt64, secret UInt64) ENGINE = MergeTree ORDER BY id"

# Without `READ ON FILE` the query is denied before it touches the directory.
if ${CLICKHOUSE_CLIENT} --user "$user" --query "$attach_query" 2>&1 | grep -qF "necessary to have the grant READ ON FILE"; then
    echo "ACCESS_DENIED"
else
    echo "UNEXPECTED: the attach was not denied"
fi
${CLICKHOUSE_CLIENT} --query "SELECT 'tables named stolen:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'stolen'"
if [ -d "${USER_FILES_PATH}/${staged_dir}" ]; then
    echo "staged directory is still in place"
else
    echo "UNEXPECTED: the staged directory is gone"
fi

# With the grant the same query attaches the data.
${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "$attach_query"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT secret FROM stolen"

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE stolen;
DROP TABLE protected;
DROP USER $user;
EOF
rm -rf "${USER_FILES_PATH:?}/${staged_dir}"
