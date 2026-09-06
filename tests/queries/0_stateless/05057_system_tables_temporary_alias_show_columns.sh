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
admin "DROP VIEW IF EXISTS ${db}.tmp_param_view"
admin "DROP TABLE IF EXISTS ${db}.tmp_param_base"
admin "
    CREATE TABLE ${db}.tmp_target
    (
        id UInt64,
        hidden_col String,
        INDEX hidden_idx hidden_col TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id;"
admin "CREATE TABLE ${db}.tmp_param_base (a Int32) ENGINE = MergeTree ORDER BY a"
# The parameter is not named after a column, so a non-empty parameter list cannot be produced from
# the column list alone.
admin "CREATE VIEW ${db}.tmp_param_view AS SELECT * FROM ${db}.tmp_param_base WHERE a = {p:Int32}"
admin "CREATE USER ${user} NOT IDENTIFIED"
admin "GRANT SELECT ON system.tables TO ${user}"
admin "GRANT SHOW TABLES ON ${db}.* TO ${user}"
admin "GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user}"
# An explicit engine needs TABLE ENGINE independently of CREATE ARBITRARY TEMPORARY TABLE, and the
# functional-test config enables table_engines_require_grant.
admin "GRANT TABLE ENGINE ON Alias TO ${user}"
# Creating an Alias requires table-level SHOW COLUMNS on the target, so the withheld state below can
# only be reached by revoking it after the alias exists.
admin "GRANT SHOW COLUMNS ON ${db}.tmp_target TO ${user}"
admin "GRANT SHOW COLUMNS ON ${db}.tmp_param_view TO ${user}"

session_query "CREATE TEMPORARY TABLE t_alias ENGINE = Alias('${db}', 'tmp_target')"
session_query "CREATE TEMPORARY TABLE t_param_alias ENGINE = Alias('${db}', 'tmp_param_view')"

# An Alias reports the target's metadata, so the first two columns describe ${db}.tmp_target and the
# parameter list describes ${db}.tmp_param_view.
# engine is never gated: it stays 1 in both arms, so a row that disappeared cannot be read as a row
# whose columns were withheld. A temporary row carries no database, and only this session can see
# these two.
probe() {
    session_query "
        SELECT
            notEmpty(create_table_query), notEmpty(skipping_indices_types), notEmpty(engine)
        FROM system.tables WHERE is_temporary AND database = '' AND name = 't_alias';"
    session_query "
        SELECT
            notEmpty(parameterized_view_parameters), notEmpty(engine)
        FROM system.tables WHERE is_temporary AND database = '' AND name = 't_param_alias';"
}

echo "--- SHOW COLUMNS on the targets: the temporary aliases expose the target metadata ---"
probe

echo "--- target SHOW COLUMNS revoked: metadata withheld, rows still emitted ---"
admin "REVOKE SHOW COLUMNS ON ${db}.tmp_target FROM ${user}"
admin "REVOKE SHOW COLUMNS ON ${db}.tmp_param_view FROM ${user}"
probe
# The interpreter refuses the same object for the same user, so the two surfaces agree.
session_query "SHOW CREATE TEMPORARY TABLE t_alias" 2>&1 | grep -o -m1 ACCESS_DENIED

admin "DROP USER ${user}"
admin "DROP TABLE ${db}.tmp_target"
admin "DROP VIEW ${db}.tmp_param_view"
admin "DROP TABLE ${db}.tmp_param_base"
