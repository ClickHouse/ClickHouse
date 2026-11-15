#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/120522
# The DeltaLake writer committed the per-file statistics one level too deep, as
# "stats":"{\"stats_json\":\"{\\\"numRecords\\\":10}\"}" instead of "stats":"{\"numRecords\":10}",
# because the kernel JSON-encodes the stats struct using its own field names. No reader found
# numRecords: SELECT count() lost the trivial-count optimization, external readers lost count(*),
# and an INSERT into a table whose protocol supports the rowTracking writer feature was rejected
# outright (the kernel selects stats.numRecords there).
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol + metaData):
# CREATE TABLE ... DeltaLakeLocal does write an initial commit, but it expresses neither partition
# columns nor writer features, which two of these three fixtures need.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_stats"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# Create an empty Delta table at $1 with the given JSON schema string ($2), partitionColumns
# array ($3) and protocol action ($4), using a minimal v0 transaction log.
bootstrap() {
    local path="$1"
    local schema="$2"
    local partition_cols="$3"
    local protocol="$4"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
${protocol}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"${schema}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

PROTOCOL_V2='{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}'
# A table that merely SUPPORTS the rowTracking writer feature is enough: the kernel gates the
# row-tracking write path on feature support, not on the delta.enableRowTracking property.
PROTOCOL_ROW_TRACKING='{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["rowTracking","domainMetadata"]}}'

# For every committed add action of the commit file(s) piped in, in add.path order: the raw
# numRecords value of add.stats and the comma-joined list of its top-level JSON keys. The key list
# pins the whole shape, so a writer that added numRecords BESIDE stats_json would still be caught.
# The log is fed on stdin because file() in clickhouse-local is confined to the current directory.
committed_stats() {
    local commits="$1"
    if [ ! -f "${commits}" ]; then
        echo "no such commit: $(basename "${commits}")"
        return
    fi
    ${CLICKHOUSE_LOCAL} --input-format=LineAsString --structure="line String" --query "
        SELECT JSONExtractRaw(stats, 'numRecords') AS num_records,
               arrayStringConcat(JSONExtractKeys(stats), ',') AS keys
        FROM (SELECT JSONExtractString(line, 'add', 'stats') AS stats,
                     JSONExtractString(line, 'add', 'path')  AS path
              FROM table
              WHERE JSONHas(line, 'add') ORDER BY path)
    " < "${commits}"
}

# The row-tracking write path DERIVES these from add.stats.numRecords: baseRowId is the previous
# high water mark plus one, and delta.rowTracking's rowIdHighWaterMark is that mark plus numRecords.
# They are absent on the ordinary append path, so this also shows which path the kernel took.
row_tracking_metadata() {
    local commits="$1"
    if [ ! -f "${commits}" ]; then
        echo "no such commit: $(basename "${commits}")"
        return
    fi
    ${CLICKHOUSE_LOCAL} --input-format=LineAsString --structure="line String" --query "
        SELECT info FROM (
            SELECT concat('baseRowId=', JSONExtractRaw(line, 'add', 'baseRowId'),
                          ' defaultRowCommitVersion=',
                          JSONExtractRaw(line, 'add', 'defaultRowCommitVersion')) AS info
            FROM table WHERE JSONHas(line, 'add')
            UNION ALL
            SELECT concat(JSONExtractString(line, 'domainMetadata', 'domain'), ' ',
                          JSONExtractString(line, 'domainMetadata', 'configuration')) AS info
            FROM table WHERE JSONHas(line, 'domainMetadata')
        ) ORDER BY info
    " < "${commits}"
}

# Cross-check every committed numRecords against the real row count of its data file. A wrong
# numRecords is worse than an absent one, because readers answer count(*) from it. Data file
# names are random UUIDs, so the report is sorted rather than emitted in log order.
check_num_records() {
    local root="$1"
    cat "${root}"/_delta_log/*.json | ${CLICKHOUSE_LOCAL} --input-format=LineAsString \
        --structure="line String" --query "
        SELECT JSONExtractString(line, 'add', 'path') AS path,
               JSONExtractInt(JSONExtractString(line, 'add', 'stats'), 'numRecords') AS committed
        FROM table WHERE JSONHas(line, 'add')
        FORMAT TSV" | while IFS=$'\t' read -r path committed; do
        actual=$(${CLICKHOUSE_LOCAL} --input-format=Parquet \
            --query "SELECT count() FROM table" < "${root}/${path}")
        if [ "${actual}" = "${committed}" ]; then
            echo "numRecords matches data file: ${committed}"
        else
            echo "MISMATCH: committed=${committed} actual=${actual}"
        fi
    done | LC_ALL=C sort
}

SCHEMA_ID='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}'
SCHEMA_ID_PART='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"part\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}'

echo "-- unpartitioned sink: each commit carries numRecords at the top level of add.stats"
bootstrap "${ROOT}/plain" "${SCHEMA_ID}" '[]' "${PROTOCOL_V2}"
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/plain') SELECT number AS id FROM numbers(10);
"
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/plain') SELECT number AS id FROM numbers(10, 3);
"
echo "commit 1:"
committed_stats "${ROOT}/plain/_delta_log/00000000000000000001.json"
echo "commit 2:"
committed_stats "${ROOT}/plain/_delta_log/00000000000000000002.json"
check_num_records "${ROOT}/plain"

echo "-- the row count is now readable from the log, so count() uses the trivial-count optimization"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE plain ENGINE = DeltaLakeLocal('${ROOT}/plain', Parquet);
    SELECT count() FROM (EXPLAIN SELECT count() FROM plain) WHERE explain LIKE '%Optimized trivial count%';
    SELECT count() FROM plain;
"

echo "-- partitioned sink: every add action of a multi-file commit carries its own row count"
bootstrap "${ROOT}/part" "${SCHEMA_ID_PART}" '["part"]' "${PROTOCOL_V2}"
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/part')
        SELECT number AS id, if(number < 3, 'a', 'b') AS part FROM numbers(10);
"
committed_stats "${ROOT}/part/_delta_log/00000000000000000001.json"
check_num_records "${ROOT}/part"

echo "-- a table whose protocol supports the rowTracking writer feature accepts the INSERT"
bootstrap "${ROOT}/rowtracking" "${SCHEMA_ID}" '[]' "${PROTOCOL_ROW_TRACKING}"
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/rowtracking') SELECT number AS id FROM numbers(5);
" 2>&1 | grep -oE "DELTA_KERNEL_ERROR" | head -1
echo "rows read back: $(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM deltaLakeLocal('${ROOT}/rowtracking')" 2>/dev/null)"
committed_stats "${ROOT}/rowtracking/_delta_log/00000000000000000001.json"
row_tracking_metadata "${ROOT}/rowtracking/_delta_log/00000000000000000001.json"
echo "-- control: the ordinary append path writes no row-tracking columns and no domain metadata"
row_tracking_metadata "${ROOT}/plain/_delta_log/00000000000000000001.json"
