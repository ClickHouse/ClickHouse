#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test: a partial `DROP PARTITION` rewrite must encode survivor partition
# values with the Avro type derived from the partition column, not from the `Field`
# tag. `UInt8/16/32` are stored as `Field::UInt64`, while their partition field is
# declared Avro `int`; emitting a `long` for an `int` field corrupted the rewritten
# survivor manifest (or failed the rewrite). A single multi-partition INSERT produces
# one manifest, so dropping one partition forces the surviving entries to be rewritten.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_uint_partition"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a UInt32, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

# One statement -> one manifest holding all three partitions.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (2, 'y'), (3, 'z')"

# Dropping partition 2 partially matches the manifest, so survivors (1, 3) are rewritten.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 2"

echo "--- survivors (expect 1 x / 3 z) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a FORMAT TSV"

echo "--- count (expect 2) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

echo "--- history operation counts (history must be readable) ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT operation, count()
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    GROUP BY operation
    ORDER BY operation
    FORMAT TSV
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
