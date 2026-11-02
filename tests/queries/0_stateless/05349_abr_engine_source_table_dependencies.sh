#!/usr/bin/env bash
# Tags: no-encrypted-storage
# Tag no-encrypted-storage: backs up to the `backups` disk, as the other database backup tests.

# The source tables of an `ABR` table are registered in both dependency graphs: in the graph of loading
# dependencies, so they are loaded before it and cannot be dropped or renamed while it exists, and in the
# graph of referential dependencies, from which `RESTORE` builds the order in which it creates the tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATABASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"

${CLICKHOUSE_CLIENT} --query "
    DROP DATABASE IF EXISTS ${DATABASE};
    CREATE DATABASE ${DATABASE};

    CREATE TABLE ${DATABASE}.src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    CREATE TABLE ${DATABASE}.src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    INSERT INTO ${DATABASE}.src_1 SELECT toDateTime('2026-01-10 00:00:00') + INTERVAL number DAY, 1 FROM numbers(10);
    INSERT INTO ${DATABASE}.src_10 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number DAY, 10 FROM numbers(20);

    CREATE TABLE ${DATABASE}.abr ENGINE = ABR('src', [1, 10], ('ts'));

    SELECT 'loading dependencies';
    SELECT arraySort(loading_dependencies_table) FROM system.tables WHERE database = '${DATABASE}' AND name = 'abr';
    SELECT loading_dependent_table FROM system.tables WHERE database = '${DATABASE}' AND name = 'src_1';
"

echo "DROP a source table"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DATABASE}.src_1" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | uniq

echo "RENAME a source table"
${CLICKHOUSE_CLIENT} --query "RENAME TABLE ${DATABASE}.src_10 TO ${DATABASE}.src_100" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | uniq

echo "DROP a source table checking the referential dependencies"
${CLICKHOUSE_CLIENT} --check_referential_table_dependencies=1 --query "DROP TABLE ${DATABASE}.src_1" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | uniq

echo "RESTORE"
${CLICKHOUSE_CLIENT} --query "
    BACKUP DATABASE ${DATABASE} TO Disk('backups', '${DATABASE}') FORMAT Null;
    DROP DATABASE ${DATABASE};
    RESTORE DATABASE ${DATABASE} FROM Disk('backups', '${DATABASE}') FORMAT Null;

    SELECT max(value) FROM ${DATABASE}.abr WHERE ts >= toDateTime('2026-01-12 00:00:00');
    SELECT max(value) FROM ${DATABASE}.abr WHERE ts >= toDateTime('2026-01-05 00:00:00');

    DROP DATABASE ${DATABASE};
"
