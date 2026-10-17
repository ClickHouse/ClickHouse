#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan, so DeltaLakeLocal is absent

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/122020
# An INSERT into a DeltaLake table that writes no data file must not create a new transaction
# log version. It used to do so on an unpartitioned table (committing a version that carried
# only commitInfo and referenced no data file) while a partitioned table committed nothing, so
# a repeated no-op append (a scheduled INSERT SELECT that selects no rows, for example) grew
# the transaction log of unpartitioned tables only. An INSERT that does write data must still
# commit, on both kinds of table.
#
# Every table below has the same two columns and takes the same INSERT, so the only difference
# between the unpartitioned and partitioned arms is partitionColumns.
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol +
# metaData), because ClickHouse cannot initialize a Delta transaction log itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_empty_commit"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# schema for (id Nullable(Int32), p Nullable(Int32)). Two columns, so a table partitioned by `p`
# still has a data column to write: partitioning by every column is rejected up front.
SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}'

# Create an empty Delta table at $1 with the given partitionColumns array ($2).
bootstrap() {
    local path="$1"
    local partition_cols="$2"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"${SCHEMA}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

insert() {
    ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query \
        "INSERT INTO FUNCTION deltaLakeLocal('$1') $2"
}

# Print the state of the table at $1: which transaction log versions exist, how many data files
# they reference in total, and how many rows read back. Deterministic: no timestamps, no UUIDs.
state() {
    local path="$1"
    echo "   log:     $(ls -1 "${path}"/_delta_log/*.json | xargs -n1 basename | LC_ALL=C sort | paste -sd' ' -)"
    echo "   adds:    $(grep -ho '"add"' "${path}"/_delta_log/*.json | wc -l)"
    echo -n "   rows:    "
    ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "SELECT count() FROM deltaLakeLocal('${path}')"
}

echo "-- unpartitioned, 0-row INSERT: no new log version (was: a version referencing no data file)"
bootstrap "${ROOT}/unpart_empty" '[]'
insert "${ROOT}/unpart_empty" "SELECT 1::Int32 AS id, 2::Int32 AS p WHERE 0"
state "${ROOT}/unpart_empty"

echo "-- unpartitioned, 0-row INSERT from an empty source: likewise"
bootstrap "${ROOT}/unpart_empty_src" '[]'
insert "${ROOT}/unpart_empty_src" "SELECT number::Int32 AS id, number::Int32 AS p FROM numbers(0)"
state "${ROOT}/unpart_empty_src"

echo "-- partitioned, 0-row INSERT: no new log version (unchanged)"
bootstrap "${ROOT}/part_empty" '["p"]'
insert "${ROOT}/part_empty" "SELECT 1::Int32 AS id, 2::Int32 AS p WHERE 0"
state "${ROOT}/part_empty"

echo "-- unpartitioned, INSERT that writes data: still committed"
bootstrap "${ROOT}/unpart_data" '[]'
insert "${ROOT}/unpart_data" "SELECT 1::Int32 AS id, 2::Int32 AS p"
state "${ROOT}/unpart_data"

echo "-- partitioned, INSERT that writes data: still committed"
bootstrap "${ROOT}/part_data" '["p"]'
insert "${ROOT}/part_data" "SELECT 1::Int32 AS id, 2::Int32 AS p"
state "${ROOT}/part_data"

echo "-- a 0-row INSERT into a table that already has data adds nothing (the no-op refresh)"
bootstrap "${ROOT}/unpart_refresh" '[]'
insert "${ROOT}/unpart_refresh" "SELECT 1::Int32 AS id, 2::Int32 AS p"
insert "${ROOT}/unpart_refresh" "SELECT 3::Int32 AS id, 4::Int32 AS p WHERE 0"
state "${ROOT}/unpart_refresh"
