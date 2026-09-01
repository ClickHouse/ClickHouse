#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="tmp_alias_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
session="tmp_alias_session_${CLICKHOUSE_TEST_UNIQUE_NAME}"

admin() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

# All of the user's requests share one server-side session, so the temporary table outlives the
# revoke that happens between two of them.
session_query() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=&session_id=${session}" -d "$1"
}

admin "DROP USER IF EXISTS ${user}"
admin "DROP TABLE IF EXISTS ${db}.tmp_target"
admin "
    CREATE TABLE ${db}.tmp_target
    (
        id UInt64,
        hidden_col String,
        INDEX hidden_idx hidden_col TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id;"
admin "CREATE USER ${user} NOT IDENTIFIED"
admin "GRANT SELECT ON system.tables TO ${user}"
admin "GRANT SHOW TABLES ON ${db}.* TO ${user}"
admin "GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user}"
# Creating an Alias requires table-level SHOW COLUMNS on the target, so the withheld state below can
# only be reached by revoking it after the alias exists.
admin "GRANT SHOW COLUMNS ON ${db}.tmp_target TO ${user}"

session_query "CREATE TEMPORARY TABLE t_alias ENGINE = Alias('${db}', 'tmp_target')"

# An Alias reports the target's metadata, so both of the first two columns describe ${db}.tmp_target.
# engine is never gated: it stays 1 in both arms, so a row that disappeared cannot be read as a row
# whose columns were withheld. A temporary row carries no database, and only this session can see
# this one.
probe() {
    session_query "
        SELECT
            notEmpty(create_table_query), notEmpty(skipping_indices_types), notEmpty(engine)
        FROM system.tables WHERE is_temporary AND database = '' AND name = 't_alias';"
}

echo "--- SHOW COLUMNS on the target: the temporary alias exposes the target's schema ---"
probe

echo "--- target SHOW COLUMNS revoked: schema withheld, row still emitted ---"
admin "REVOKE SHOW COLUMNS ON ${db}.tmp_target FROM ${user}"
probe
# The interpreter refuses the same object for the same user, so the two surfaces agree.
session_query "SHOW CREATE TEMPORARY TABLE t_alias" 2>&1 | grep -o -m1 ACCESS_DENIED

admin "DROP USER ${user}"
admin "DROP TABLE ${db}.tmp_target"
