#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, zookeeper
# - no-fasttest: `IcebergLocal` requires the `USE_AVRO` build option.
# - no-replicated-database: `ON CLUSTER` is not allowed in a `Replicated` database.
# - zookeeper: `ON CLUSTER` queries go through the distributed DDL queue.

# A `CREATE TABLE ... ON CLUSTER` query is not executed on the initiator: every host runs it from the
# distributed DDL queue. Such a query introduces a fresh definition on each host, so the Parquet
# `field_id` settings in its `SETTINGS` clause must be validated there exactly as for a local `CREATE`,
# instead of producing a table that fails every `INSERT`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Print only the error code name of a failed query, or `OK`.
outcome()
{
    local out
    if out=$(${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=throw --query "$1" 2>&1)
    then
        echo "OK"
    else
        # The error text of a distributed DDL query can contain the code name several times on one line.
        if echo "$out" | grep -q 'BAD_ARGUMENTS'
        then
            echo "BAD_ARGUMENTS"
        else
            echo "$out"
        fi
    fi
}

TABLE="t_parquet_field_ids_on_cluster_${CLICKHOUSE_DATABASE}"
ICEBERG_PATH="${USER_FILES_PATH}/${TABLE}_iceberg/"
rm -rf "${ICEBERG_PATH}"

echo "File with an unknown column in the field_id map"
outcome "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost (x Int64) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'missing': '1'}"
echo "File with a map that does not cover every column"
outcome "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost (x Int64, y Int64) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '1'}"
echo "Iceberg with auto-assigned field_ids"
outcome "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost (x Int64) ENGINE = IcebergLocal('${ICEBERG_PATH}') SETTINGS output_format_parquet_auto_assign_field_ids = 1"
echo "No table was created"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"

echo "A valid definition is accepted and writable"
outcome "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost (x Int64, y Int64) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '1', 'y': '2'}"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.${TABLE} VALUES (1, 2)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.${TABLE} SYNC"
rm -rf "${ICEBERG_PATH}"
