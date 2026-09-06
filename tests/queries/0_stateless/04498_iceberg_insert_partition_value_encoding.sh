#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test: manifest partition values must be encoded with the Avro type derived
# from the partition column type, not from the `Field` tag. `UInt8/16/32` values are
# stored as `Field::UInt64`, while their manifest partition field is declared Avro `int`,
# so keying on the tag emitted a `long` datum for an `int` field and the INSERT failed.
# `DateTime64` values are stored as `Field::Decimal64` and must be unwrapped to the
# underlying ticks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_partition_encoding"
TABLE_PATH_BASE="${USER_FILES_PATH}/${TABLE}"

trap 'rm -rf "${TABLE_PATH_BASE}"* 2>/dev/null' EXIT

echo "--- UInt32 partition (Avro int) ---"
TABLE_PATH="${TABLE_PATH_BASE}_uint32/"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a UInt32, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (2, 'y'), (3, 'z')"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT b FROM ${TABLE} WHERE a = 2 FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"

echo "--- DateTime64 partition (Avro long from Decimal64 ticks) ---"
TABLE_PATH="${TABLE_PATH_BASE}_datetime64/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (t DateTime64(6, 'UTC'), v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (t)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES ('2024-01-01 00:00:00.123456', 'a'), ('2024-06-15 12:30:00.654321', 'b')
"
${CLICKHOUSE_CLIENT} --query "SELECT t, v FROM ${TABLE} ORDER BY t FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"

echo "--- Bucket partition (Avro int from Field::UInt64) ---"
TABLE_PATH="${TABLE_PATH_BASE}_bucket/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64, v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY icebergBucket(4, id)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM ${TABLE} ORDER BY id FORMAT TSV"

# The manifest `data_file.partition` record must be declared from the persisted partition
# spec: the field name and `field-id` written into the manifest Avro schema have to match
# the spec in the table metadata (the spec assigns ids from 1001, so a manifest that falls
# back to the `1000 + i` default would not match). The reader is positional and would not
# notice a mismatch, so inspect the written files directly.
SPEC_FIELD_ID=$(grep -ho '"field-id" : [0-9]*' "${TABLE_PATH}metadata/"*.json | grep -o '[0-9]*$' | sort -u)
echo "spec field-id: ${SPEC_FIELD_ID}"
for manifest in "${TABLE_PATH}metadata/"*.avro; do
    # Skip manifest lists: only manifest files carry the `data_file.partition` record.
    if grep -a -q '"data_file"' "${manifest}"; then
        # The Avro `int` partition field must be the spec field, keyed by the spec `field-id`.
        grep -a -o "{\"field-id\":${SPEC_FIELD_ID},\"name\":\"[^\"]*\",\"type\":\"int\"}" "${manifest}" | sed 's/^/manifest schema: /'
        ${CLICKHOUSE_CLIENT} --query "SELECT DISTINCT 'manifest partition names: ' || arrayStringConcat(tupleNames(data_file.partition), ',') FROM file('${manifest}', Avro) FORMAT TSV"
    fi
done | sort -u
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"

echo "--- Float32 partition (Avro float from Field::Float64) ---"
TABLE_PATH="${TABLE_PATH_BASE}_float32/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (f Float32, v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (f)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1.25, 'a'), (2.5, 'b')"
${CLICKHOUSE_CLIENT} --query "SELECT f, v FROM ${TABLE} ORDER BY f FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
