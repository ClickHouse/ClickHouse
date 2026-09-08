#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="chain_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
mid_user="chain_mid_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
col_user="chain_col_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Build `chain_a1 -> chain_a2 -> chain_a3 -> chain_base`. Every CREATE names a target that is either
# absent or not an Alias, which is the only order the point-in-time Alias -> Alias rejection accepts.
# The user holds everything on the three alias names and nothing at all on `chain_base`.
${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${user};
    DROP USER IF EXISTS ${mid_user};
    DROP USER IF EXISTS ${col_user};
    DROP TABLE IF EXISTS chain_a1;
    DROP TABLE IF EXISTS chain_a2;
    DROP TABLE IF EXISTS chain_a3;
    DROP TABLE IF EXISTS chain_base;

    CREATE TABLE chain_base
    (
        secret_id UInt64,
        secret_payload String,
        INDEX secret_idx secret_id TYPE minmax GRANULARITY 1,
        PROJECTION secret_proj (SELECT secret_id ORDER BY secret_id),
        CONSTRAINT secret_positive CHECK secret_id > 0
    )
    ENGINE = MergeTree ORDER BY secret_id;
    INSERT INTO chain_base VALUES (1, 'first'), (2, 'second');

    CREATE TABLE chain_a1 ENGINE = Alias(currentDatabase(), 'chain_a2');
    CREATE TABLE chain_a2 ENGINE = Alias(currentDatabase(), 'chain_a3');
    CREATE TABLE chain_a3 ENGINE = Alias(currentDatabase(), 'chain_base');

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON chain_a1 TO ${user};
    GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON chain_a2 TO ${user};
    GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON chain_a3 TO ${user};
    -- These three are outside the system.tables/system.columns carve-out, so they need an explicit
    -- grant once select_from_system_db_requires_grant is on.
    GRANT SELECT ON system.constraints TO ${user};
    GRANT SELECT ON system.projections TO ${user};
    GRANT SELECT ON system.data_skipping_indices TO ${user};

    CREATE USER ${mid_user} NOT IDENTIFIED;
    GRANT SHOW TABLES, SHOW COLUMNS ON chain_a1 TO ${mid_user};
    GRANT SHOW TABLES, SHOW COLUMNS ON chain_a2 TO ${mid_user};
    GRANT SHOW TABLES, SHOW COLUMNS ON chain_base TO ${mid_user};

    CREATE USER ${col_user} NOT IDENTIFIED;
    GRANT SHOW TABLES, SHOW COLUMNS ON chain_a1 TO ${col_user};
    GRANT SHOW TABLES, SHOW COLUMNS ON chain_a2 TO ${col_user};
    GRANT SHOW TABLES, SHOW COLUMNS ON chain_a3 TO ${col_user};
    GRANT SHOW COLUMNS(secret_id) ON chain_base TO ${col_user};
"

echo "Test DESCRIBE through the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "DESCRIBE TABLE chain_a1;" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "Test system.columns through the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'chain_a1';
"

echo "Test resolved and declared system.tables columns through the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT
        empty(sorting_key),
        isNull(total_rows),
        position(create_table_query, 'chain_a2') > 0,
        position(create_table_query, 'chain_base') = 0,
        position(create_table_query, 'secret_') = 0,
        notEmpty(engine_full)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'chain_a1';
"

echo "Test SHOW CREATE TABLE exposes the declared target"
ddl=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE TABLE chain_a1 FORMAT TSVRaw" 2>&1)
echo "$ddl" | grep -c "ENGINE = Alias"
echo "$ddl" | grep -c "'chain_a2'"
echo "$ddl" | grep -cE "chain_base|secret_"

# Control: `chain_a3` was created while `chain_base` already existed, so its own stored definition names
# that table (and, until #115141 lands, carries its columns too). The same filter over it must match, or
# the `0` above would be proving nothing.
echo "Control: the same filter over a definition that does name the target"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE chain_a3 FORMAT TSVRaw" 2>&1 | grep -qE "chain_base|secret_" && echo "match"

echo "Test DESCRIBE of the last alias in the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "DESCRIBE TABLE chain_a3;" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "Test SELECT through the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "SELECT count() FROM chain_a1;" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "Test hasColumnInTable through the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "SELECT hasColumnInTable(currentDatabase(), 'chain_a1', 'secret_payload');" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "Test system.projections, system.constraints and system.data_skipping_indices through the chain"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT
        (SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'chain_a1'),
        (SELECT count() FROM system.constraints WHERE database = currentDatabase() AND table = 'chain_a1'),
        (SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'chain_a1');
"

# `mid_user` holds the grant on the declared target `chain_a2` and on the final table `chain_base`, and
# nothing on the hop between them, so a check that skips intermediate names still passes.
echo "Test DESCRIBE with only the middle alias of the chain ungranted"
${CLICKHOUSE_CLIENT} --user="${mid_user}" --query "DESCRIBE TABLE chain_a1;" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "Test system.columns with a column-level grant on the final table"
${CLICKHOUSE_CLIENT} --user="${col_user}" --query "
    SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'chain_a1' ORDER BY name;
"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON chain_base TO ${user};"

echo "Test DESCRIBE through the chain with the final target granted"
${CLICKHOUSE_CLIENT} --user="${user}" --query "DESCRIBE TABLE chain_a1;" | cut -f1,2

echo "Test system.columns through the chain with the final target granted"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'chain_a1';
"

echo "Test resolved and declared system.tables columns through the chain with the final target granted"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT
        sorting_key,
        total_rows,
        position(create_table_query, 'chain_a2') > 0
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'chain_a1';
"

echo "Test hasColumnInTable through the chain with the final target granted"
${CLICKHOUSE_CLIENT} --user="${user}" --query "SELECT hasColumnInTable(currentDatabase(), 'chain_a1', 'secret_payload');"

echo "Test system.projections, system.constraints and system.data_skipping_indices with the final target granted"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT
        (SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'chain_a1'),
        (SELECT count() FROM system.constraints WHERE database = currentDatabase() AND table = 'chain_a1'),
        (SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'chain_a1');
"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE chain_a1;
    DROP TABLE chain_a2;
    DROP TABLE chain_a3;
    DROP TABLE chain_base;
    DROP USER ${user};
    DROP USER ${mid_user};
    DROP USER ${col_user};
"
