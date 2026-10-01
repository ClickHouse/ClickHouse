#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

access_username="access_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
alias_database="alias_db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
target_database="target_db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
shortcut_alias_table="shortcut_alias_${CLICKHOUSE_TEST_UNIQUE_NAME}"
shortcut_buffer_alias_table="shortcut_buffer_alias_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${access_username};
    DROP DATABASE IF EXISTS ${alias_database};
    DROP DATABASE IF EXISTS ${target_database};

    CREATE USER ${access_username} NOT IDENTIFIED;
    GRANT SELECT ON system.completions TO ${access_username};
    GRANT SELECT ON system.constraints TO ${access_username};
    GRANT SELECT ON system.data_skipping_indices TO ${access_username};
    GRANT SELECT ON system.projections TO ${access_username};
"

# Test database-level access shortcuts with a cross-database `Alias`
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE DATABASE ${alias_database};
    CREATE DATABASE ${target_database};

    CREATE TABLE ${target_database}.target
    (
        id UInt64,
        CONSTRAINT id_not_zero CHECK id != 0,
        PROJECTION id_projection (SELECT id ORDER BY id),
        INDEX id_idx id TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY id;
    INSERT INTO ${target_database}.target VALUES (1);

    CREATE TABLE ${target_database}.target_buffer (id UInt64)
    ENGINE = Buffer('${target_database}', 'target', 1, 1000, 1000, 1000, 1000, 1000000, 1000000);
    INSERT INTO ${target_database}.target_buffer VALUES (2);

    CREATE TABLE ${alias_database}.${shortcut_alias_table} ENGINE = Alias('${target_database}', 'target');
    CREATE TABLE ${alias_database}.${shortcut_buffer_alias_table} ENGINE = Alias('${target_database}', 'target_buffer');
    GRANT SHOW TABLES ON ${alias_database}.* TO ${access_username};
    GRANT SHOW COLUMNS ON ${alias_database}.* TO ${access_username};
"

echo "Test system.tables with an Alias database-level grant"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count()
         FROM system.tables
         WHERE database = '${alias_database}' AND name = '${shortcut_alias_table}'),
        (SELECT countIf(
             empty(create_table_query)
             AND empty(engine_full)
             AND empty(sorting_key)
             AND empty(skipping_indices_types)
             AND isNull(total_rows)
             AND isNull(total_bytes)
             AND isNull(total_bytes_uncompressed)
             AND empty(data_paths)
             AND empty(storage_policy))
         FROM system.tables
         WHERE database = '${alias_database}' AND name = '${shortcut_alias_table}'),
        (SELECT count()
         FROM system.tables
         WHERE database = '${alias_database}' AND name = '${shortcut_buffer_alias_table}'),
        (SELECT countIf(isNull(lifetime_rows) AND isNull(lifetime_bytes))
         FROM system.tables
         WHERE database = '${alias_database}' AND name = '${shortcut_buffer_alias_table}');
"

echo "Test other metadata tables with an Alias database-level grant"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count() FROM system.columns WHERE database = '${alias_database}' AND table = '${shortcut_alias_table}'),
        (SELECT count() FROM system.constraints WHERE database = '${alias_database}' AND table = '${shortcut_alias_table}'),
        (SELECT count() FROM system.projections WHERE database = '${alias_database}' AND table = '${shortcut_alias_table}'),
        (SELECT count() FROM system.data_skipping_indices WHERE database = '${alias_database}' AND table = '${shortcut_alias_table}'),
        (SELECT count() FROM system.completions WHERE context = 'column' AND belongs = '${shortcut_alias_table}');
"

# Test an `Alias` in a database with `lazy_load_tables`, which must not be deferred behind a proxy.
# `system.completions.belongs` is the bare table name, so the alias needs a server-unique name for
# the completions count below to mean this table and not a same-named alias in another database.
lazy_database="lazy_db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
lazy_alias_table="lazy_alias_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE DATABASE ${lazy_database} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${lazy_database}.${lazy_alias_table} ENGINE = Alias('${target_database}', 'target');
    DETACH DATABASE ${lazy_database};
    ATTACH DATABASE ${lazy_database};
    GRANT SHOW TABLES ON ${lazy_database}.* TO ${access_username};
    GRANT SHOW COLUMNS ON ${lazy_database}.* TO ${access_username};
"

echo "Test an Alias in a lazily loaded database is not deferred"
${CLICKHOUSE_CLIENT} --query "
    SELECT engine FROM system.tables WHERE database = '${lazy_database}' AND name = '${lazy_alias_table}';
"

lazy_probes() {
    echo "Test DESCRIBE of a lazily loaded Alias without target permission$1"
    ${CLICKHOUSE_CLIENT} --user="${access_username}" --query "DESCRIBE TABLE ${lazy_database}.${lazy_alias_table};" 2>&1 | grep -o "ACCESS_DENIED" | head -1

    echo "Test SHOW CREATE of a lazily loaded Alias without target permission$1"
    ${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SHOW CREATE TABLE ${lazy_database}.${lazy_alias_table};" 2>&1 | grep -o "ACCESS_DENIED" | head -1

    echo "Test metadata tables for a lazily loaded Alias without target permission$1"
    ${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
        SELECT
            (SELECT countIf(empty(create_table_query) AND empty(engine_full))
             FROM system.tables
             WHERE database = '${lazy_database}' AND name = '${lazy_alias_table}'),
            (SELECT count() FROM system.columns WHERE database = '${lazy_database}' AND table = '${lazy_alias_table}'),
            (SELECT count() FROM system.constraints WHERE database = '${lazy_database}' AND table = '${lazy_alias_table}'),
            (SELECT count() FROM system.projections WHERE database = '${lazy_database}' AND table = '${lazy_alias_table}'),
            (SELECT count() FROM system.data_skipping_indices WHERE database = '${lazy_database}' AND table = '${lazy_alias_table}'),
            (SELECT count() FROM system.completions WHERE context = 'column' AND belongs = '${lazy_alias_table}');
    "
}

lazy_probes ""

# Before the fix a first access materialized the proxy's nested storage while the catalog kept
# handing out the proxy, so repeat the battery after one: the answers must not depend on it.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${lazy_database}.${lazy_alias_table} FORMAT Null;"
lazy_probes " after a first access"
${CLICKHOUSE_CLIENT} --query "
    DROP DATABASE ${lazy_database};
    DROP DATABASE ${alias_database};
    DROP DATABASE ${target_database};
    DROP USER ${access_username};
"
