#!/usr/bin/env bash

# `CREATE TABLE x AS y` inherits the storage definition of `y`, the credentials in it included. They are
# masked as `[HIDDEN]` in `SHOW CREATE TABLE`, so a user who may only see the schema of `y` must not be
# able to read through them what the SELECT privilege on `y` denies: inheriting a definition that can
# carry credentials requires SELECT on the source table, while any other one still requires only
# SHOW COLUMNS.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

# The URLs are never contacted: only the definition of the source table is copied.
${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT TABLE ENGINE ON MergeTree TO ${user};
    GRANT URL ON *.* TO ${user};

    CREATE TABLE ${db}.local_src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${db}.url_src (id UInt64) ENGINE = URL('http://user:password@127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.url_src_no_password (id UInt64) ENGINE = URL('http://127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.function_src (id UInt64) AS url('http://user:password@127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.function_src_no_password (id UInt64) AS url('http://127.0.0.1:1/', 'CSV');

    GRANT SHOW COLUMNS ON ${db}.local_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.url_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.url_src_no_password TO ${user};
    GRANT SHOW COLUMNS ON ${db}.function_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.function_src_no_password TO ${user};
"

# Prints either the missing privilege or the engine of the copy that was created.
function try_copy()
{
    echo "-- copy_of_${1}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.copy_of_${1} AS ${db}.${1}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'copy_of_${1}'"
}

echo "with SHOW COLUMNS only:"
try_copy local_src
try_copy url_src
# An engine that can carry credentials requires SELECT even when this definition holds none.
try_copy url_src_no_password
try_copy function_src
# A table function is judged by its arguments, so one without credentials is copied as before.
try_copy function_src_no_password

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "
    GRANT SELECT ON ${db}.url_src TO ${user};
    GRANT SELECT ON ${db}.url_src_no_password TO ${user};
    GRANT SELECT ON ${db}.function_src TO ${user};
"
try_copy url_src
try_copy url_src_no_password
try_copy function_src

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
