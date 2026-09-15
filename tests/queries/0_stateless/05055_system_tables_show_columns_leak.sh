#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
dbx="${CLICKHOUSE_DATABASE}_x"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dep_user="dep_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${user}, ${dep_user};
    DROP DATABASE IF EXISTS ${dbx};
    CREATE DATABASE ${dbx};

    CREATE TABLE ${db}.secret
    (
        x UInt64,
        public_col String,
        hidden_col String,
        INDEX hidden_idx hidden_col TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree PARTITION BY x % 2 ORDER BY (x, public_col) SAMPLE BY x
    COMMENT 'table comment';

    CREATE VIEW ${db}.pview AS SELECT hidden_col FROM ${db}.secret WHERE x = {pid:UInt64};
    CREATE MATERIALIZED VIEW ${db}.dep_view ENGINE = Memory AS SELECT hidden_col FROM ${db}.secret;
    CREATE MATERIALIZED VIEW ${dbx}.cross_dep ENGINE = Memory AS SELECT hidden_col FROM ${db}.secret;

    CREATE TABLE ${db}.dict_src (id UInt64, v String) ENGINE = Memory;
    CREATE TABLE ${dbx}.hidden_src (id UInt64, v String) ENGINE = Memory;
    CREATE DICTIONARY ${db}.local_dict (id UInt64, v String) PRIMARY KEY id
        SOURCE(CLICKHOUSE(DB '${db}' TABLE 'dict_src')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
    CREATE DICTIONARY ${dbx}.cross_dict (id UInt64, v String) PRIMARY KEY id
        SOURCE(CLICKHOUSE(DB '${db}' TABLE 'dict_src')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
    CREATE DICTIONARY ${db}.leaky_dict (id UInt64, v String) PRIMARY KEY id
        SOURCE(CLICKHOUSE(DB '${dbx}' TABLE 'hidden_src')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
    -- Reports engine 'Dictionary' exactly like local_dict above, but it is a plain table whose
    -- CREATE query carries a column list of its own, so SHOW CREATE TABLE governs it.
    CREATE TABLE ${db}.dict_engine_tbl (id UInt64, v String) ENGINE = Dictionary('${db}.local_dict');

    CREATE TABLE ${db}.local_target (hidden_col String) ENGINE = Memory;
    CREATE TABLE ${dbx}.mv_target (hidden_col String) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${db}.mv_local TO ${db}.local_target AS SELECT hidden_col FROM ${db}.secret;
    CREATE MATERIALIZED VIEW ${db}.mv_cross TO ${dbx}.mv_target AS SELECT hidden_col FROM ${db}.secret;

    CREATE USER ${user} NOT IDENTIFIED;
    CREATE USER ${dep_user} NOT IDENTIFIED;
    GRANT SELECT ON system.tables TO ${user}, ${dep_user};
    GRANT SELECT ON information_schema.views TO ${user};
    GRANT SHOW TABLES ON ${db}.* TO ${dep_user};
    -- Keep the pview/dep_view rows visible in every scenario below, so that a blank column
    -- there is this gate withholding it and never the SHOW TABLES row filter hiding the row.
    GRANT SHOW TABLES ON ${db}.pview TO ${user};
    GRANT SHOW TABLES ON ${db}.dep_view TO ${user};
"

# One probe over every gated schema column of ${db}.secret, plus the two gated columns that
# only exist on another object (a parameterized view, and a view's SELECT).
schema_probe() {
    ${CLICKHOUSE_CLIENT} --user="${user}" --query "
        SELECT
            notEmpty(create_table_query), notEmpty(engine_full), notEmpty(partition_key),
            notEmpty(sorting_key), notEmpty(primary_key), notEmpty(sampling_key),
            notEmpty(skipping_indices_types)
        FROM system.tables WHERE database = '${db}' AND name = 'secret';
        SELECT notEmpty(parameterized_view_parameters) FROM system.tables
            WHERE database = '${db}' AND name = 'pview';
        SELECT notEmpty(as_select) FROM system.tables
            WHERE database = '${db}' AND name = 'dep_view';
    "
}

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(public_col) ON ${db}.secret TO ${user};"
echo "--- column-scoped SELECT only: row visible, schema hidden ---"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT name FROM system.tables WHERE database = '${db}' AND name = 'secret';"
schema_probe
# DESCRIBE and SHOW CREATE TABLE refuse this user, so system.tables agreeing is the invariant.
${CLICKHOUSE_CLIENT} --user="${user}" --query "DESCRIBE ${db}.secret" 2>&1 | grep -o -m1 ACCESS_DENIED
${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE TABLE ${db}.secret" 2>&1 | grep -o -m1 ACCESS_DENIED

echo "--- SHOW COLUMNS granted: schema visible ---"
${CLICKHOUSE_CLIENT} --query "
    GRANT SHOW COLUMNS ON ${db}.secret TO ${user};
    GRANT SHOW COLUMNS ON ${db}.pview TO ${user};
    GRANT SHOW COLUMNS ON ${db}.dep_view TO ${user};"
schema_probe

echo "--- table-level SELECT implies SHOW COLUMNS: schema visible ---"
${CLICKHOUSE_CLIENT} --query "
    REVOKE SHOW COLUMNS ON ${db}.secret FROM ${user};
    REVOKE SHOW COLUMNS ON ${db}.pview FROM ${user};
    REVOKE SHOW COLUMNS ON ${db}.dep_view FROM ${user};
    REVOKE SELECT ON ${db}.secret FROM ${user};
    GRANT SELECT ON ${db}.secret TO ${user};
    GRANT SELECT ON ${db}.pview TO ${user};
    GRANT SELECT ON ${db}.dep_view TO ${user};"
schema_probe

echo "--- SHOW TABLES only: schema hidden, and the gate reaches information_schema ---"
${CLICKHOUSE_CLIENT} --query "
    REVOKE SELECT ON ${db}.secret FROM ${user};
    REVOKE SELECT ON ${db}.pview FROM ${user};
    REVOKE SELECT ON ${db}.dep_view FROM ${user};
    GRANT SHOW TABLES ON ${db}.* TO ${user};"
schema_probe
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT empty(view_definition) FROM information_schema.views
    WHERE table_schema = '${db}' AND table_name = 'dep_view';"

echo "--- a dictionary's CREATE query follows SHOW CREATE DICTIONARY, not SHOW COLUMNS ---"
# ${user} still holds only SHOW TABLES on ${db}.*. A dictionary row carries no key expressions and
# no secondary indices, so partition_key and skipping_indices_types read 0 in both arms: they pin
# that nothing starts emitting them, not the width of the SHOW DICTIONARIES disjunct, which the
# create_table_query transition and the dict_engine_tbl control establish. dict_engine_tbl (a plain
# table declared as ENGINE = Dictionary, granted the same privilege) stays withheld entirely.
dictionary_probe() {
    ${CLICKHOUSE_CLIENT} --user="${user}" --query "
        SELECT notEmpty(create_table_query), notEmpty(partition_key), notEmpty(skipping_indices_types)
        FROM system.tables WHERE database = '${db}' AND name = 'local_dict';
        SELECT notEmpty(create_table_query) FROM system.tables
            WHERE database = '${db}' AND name = 'dict_engine_tbl';
    "
}
dictionary_probe
${CLICKHOUSE_CLIENT} --query "
    GRANT SHOW DICTIONARIES ON ${db}.local_dict TO ${user};
    GRANT SHOW DICTIONARIES ON ${db}.dict_engine_tbl TO ${user};"
dictionary_probe
# Each row above matches what the interpreter hands the same user for the same object.
${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE DICTIONARY ${db}.local_dict" 2>&1 | grep -o -m1 'CREATE DICTIONARY'
${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE TABLE ${db}.dict_engine_tbl" 2>&1 | grep -o -m1 ACCESS_DENIED

echo "--- another database's table names are filtered out of the dependency arrays ---"
# Database names are random per run, so every assertion is a membership test. The `hidden` flags
# come first: run as admin they must all be 1, otherwise the cross-database edges are absent from
# the fixture and the restricted arm below would agree for the wrong reason -- and for the target
# pair the admin arm is also what proves both halves are populated before the restricted arm reads
# them. `visible` is the same-database edge that must survive the filter. The two target columns are
# filled by separate inserts, so each is asserted on its own.
dependency_probe() {
    ${CLICKHOUSE_CLIENT} "$@" --query "
        SELECT
            has(dependencies_table, 'cross_dep') AS hidden_dependent_view,
            has(dependencies_table, 'dep_view') AS visible_dependent_view
        FROM system.tables WHERE database = '${db}' AND name = 'secret';
        SELECT
            has(loading_dependent_table, 'cross_dict') AS hidden_loading_dependent,
            has(loading_dependent_table, 'local_dict') AS visible_loading_dependent
        FROM system.tables WHERE database = '${db}' AND name = 'dict_src';
        SELECT has(loading_dependencies_database, '${dbx}') AS hidden_loading_dependency
        FROM system.tables WHERE database = '${db}' AND name = 'leaky_dict';
        SELECT has(loading_dependencies_table, 'dict_src') AS visible_loading_dependency
        FROM system.tables WHERE database = '${db}' AND name = 'local_dict';
        SELECT target_database = '${dbx}' AS hidden_target_database,
               target_table = 'mv_target' AS hidden_target_table
        FROM system.tables WHERE database = '${db}' AND name = 'mv_cross';
        SELECT target_database = '${db}' AS visible_target_database,
               target_table = 'local_target' AS visible_target_table
        FROM system.tables WHERE database = '${db}' AND name = 'mv_local';
    "
}
dependency_probe
dependency_probe "--user=${dep_user}"

echo "--- each dependency pair stays index-aligned ---"
${CLICKHOUSE_CLIENT} --user="${dep_user}" --query "
    SELECT
        length(dependencies_database) = length(dependencies_table),
        length(loading_dependencies_database) = length(loading_dependencies_table),
        length(loading_dependent_database) = length(loading_dependent_table)
    FROM system.tables WHERE database = '${db}' AND name IN ('secret', 'dict_src', 'leaky_dict')
    ORDER BY name;"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER ${user}, ${dep_user};
    -- These two live in \$db but reference \$dbx, so they block dropping it.
    DROP DICTIONARY ${db}.leaky_dict;
    DROP TABLE ${db}.mv_cross;
    DROP DATABASE ${dbx};
"
