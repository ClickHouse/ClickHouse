#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105852.
# When the partition column is `Nullable(T)`, the Iceberg manifest must encode
# the partition field as an Avro `["null", T]` union so that NULL partition
# values round-trip correctly. Before the fix, the manifest schema declared the
# field as a plain `T` and NULL values were silently written as the default
# value of `T` (e.g. `0` for `int`), so external readers (Spark) saw `0` where
# the writer meant `NULL`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Nullable(Int32))
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY c0
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (NULL)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (42)"

# Round-trip via the Iceberg engine itself: NULL must come back as NULL, not 0.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY isNull(c0) DESC, c0"

# Inspect the per-file manifests directly. The partition field's value in the
# Avro tuple is what Spark and other Iceberg readers see; if the bug returns,
# the NULL value will appear as 0.
for manifest in $(find "${TABLE_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f); do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT tupleElement(data_file, 'partition') AS part
        FROM file('${manifest}', Avro)
    "
done | sort
