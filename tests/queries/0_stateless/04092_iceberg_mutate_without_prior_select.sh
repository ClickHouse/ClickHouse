#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test: ALTER TABLE UPDATE on an Iceberg table used to throw a
# LOGICAL_ERROR exception ("Can't extract iceberg table state from storage
# snapshot") when no SELECT or INSERT preceded the mutation in the same server
# lifetime.
# The root cause was that `StorageObjectStorage::mutate` did not call
# `updateExternalDynamicMetadataIfExists` before reading data, so the storage
# snapshot lacked the `datalake_table_state` required by `IcebergMetadata::iterate`.

# Use `IcebergS3` rather than `IcebergLocal`, because the local object storage
# becomes read-only after `ATTACH` (it honours the `is_readonly` flag passed to
# `createObjectStorage`), which would block the mutation. S3 ignores that flag.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="04092_iceberg_mutate/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 String)
    ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')
"

# Populate the Iceberg data files in S3.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES ('a')"

# DETACH + ATTACH rebuilds the storage object from the stored CREATE statement,
# so its in-memory metadata has no `datalake_table_state` loaded — the same
# state as after a server restart. The Iceberg files in S3 are preserved.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"

# This mutation is the first data-reading operation on the freshly attached
# storage — no prior SELECT or INSERT has called
# `updateExternalDynamicMetadataIfExists`.
# `validate_mutation_query = 0` is required to suppress the validate stage
# of `MutationsInterpreter`, which would otherwise construct an
# `InterpreterSelectQuery` whose constructor calls
# `updateExternalDynamicMetadataIfExists` as a side effect and masks the bug.
# Without the fix, this throws a `LOGICAL_ERROR` exception.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --validate_mutation_query=0 --query "ALTER TABLE ${TABLE} UPDATE c0 = 'b' WHERE TRUE"

${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
