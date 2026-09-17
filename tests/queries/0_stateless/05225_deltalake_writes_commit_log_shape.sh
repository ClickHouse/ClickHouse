#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Lint of the `_delta_log` entries ClickHouse commits, against the Delta protocol's writer
# requirements (https://github.com/delta-io/delta/blob/master/PROTOCOL.md):
#   * one version file per INSERT, named `%020d.json`, versions contiguous;
#   * every line is a single action object; at most one `metaData`/`protocol` per commit;
#   * every `add` has a relative, URI-style `path` (no leading `/`, no scheme), `size` equal to the
#     object size, `partitionValues` whose keys are exactly the partition columns, `dataChange`,
#     `modificationTime`; the referenced Parquet file holds exactly the rows of its partition;
#   * a second identical INSERT appends again (the writer is not idempotent: documented);
#   * a 0-row INSERT: on an unpartitioned table it currently commits an empty version (a commit with
#     no `add`, legal per the protocol but elided by Spark), on a partitioned table it commits
#     nothing. The asymmetry is recorded here so that a change in either direction is deliberate.
# Any Spark/delta-rs reader failure on ClickHouse-written tables starts with one of these being
# off, so the lint is the cheapest place to catch it.
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol + metaData).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_log_shape"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

bootstrap() {
    local path="$1"
    local schema="$2"
    local partition_cols="$3"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"${schema}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

versions() {
    (cd "$1/_delta_log" && ls -- *.json | tr '\n' ' ')
    echo
}

# Lint one commit file. Everything the reference must not depend on (UUIDs, timestamps, sizes)
# is reduced to a boolean or a count.
lint_commit() {
    local table="$1"
    local version="$2"
    local partition_cols="$3"   # ClickHouse array literal, e.g. ['p'] or []
    local log="${table}/_delta_log/${version}.json"
    echo "commit ${version}:"
    ${CLICKHOUSE_LOCAL} --query "
        WITH JSONExtractKeys(line) AS keys
        SELECT
            'actions: ' || arrayStringConcat(arraySort(groupArray(keys[1])), ','),
            'every line is a single action: ' || toString(min(length(keys) = 1)),
            'metaData actions: ' || toString(countIf(keys[1] = 'metaData')),
            'protocol actions: ' || toString(countIf(keys[1] = 'protocol'))
        FROM file('${log}', LineAsString)
        FORMAT TSVRaw
    " | tr '\t' '\n'
    ${CLICKHOUSE_LOCAL} --query "
        WITH
            JSONExtractString(line, 'add', 'path') AS path,
            JSONExtractKeys(line, 'add', 'partitionValues') AS pv_keys
        SELECT
            'add actions: ' || toString(count()),
            'relative paths: ' || toString(min(path NOT LIKE '/%' AND path NOT LIKE '%://%' AND path != '')),
            'partitionValues keys == partition columns: ' || toString(min(arraySort(pv_keys) = arraySort(${partition_cols}))),
            'dataChange: ' || toString(min(JSONExtractBool(line, 'add', 'dataChange'))),
            'modificationTime set: ' || toString(min(JSONExtractInt(line, 'add', 'modificationTime') > 0)),
            'size set: ' || toString(min(JSONExtractInt(line, 'add', 'size') > 0))
        FROM file('${log}', LineAsString)
        WHERE JSONHas(line, 'add')
        FORMAT TSVRaw
    " | tr '\t' '\n'
    # Cross-check every add against the data file it references: committed size == object size,
    # and the file holds a plausible share of the rows (all rows of one INSERT are spread over the
    # adds of that commit).
    ${CLICKHOUSE_LOCAL} --query "
        SELECT
            JSONExtractString(line, 'add', 'path'),
            JSONExtractInt(line, 'add', 'size')
        FROM file('${log}', LineAsString)
        WHERE JSONHas(line, 'add')
        ORDER BY 1
        FORMAT TSV
    " | while IFS=$'\t' read -r path size; do
        # add.path is URI-encoded (e.g. %20 for a space); decode it to locate the object.
        decoded=$(${CLICKHOUSE_LOCAL} --query "SELECT decodeURLComponent('${path}')")
        ${CLICKHOUSE_LOCAL} --query "
            SELECT
                'size matches object: ' || toString(any(_size) = ${size}),
                'rows in file: ' || toString(count())
            FROM file('${table}/${decoded}', Parquet)
            FORMAT TSVRaw
        " | tr '\t' '\n'
    done
}

SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'

echo "==== unpartitioned"
UNPART="${ROOT}/unpart"
bootstrap "${UNPART}" "${SCHEMA}" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${UNPART}') SELECT number AS id, 'p' || toString(number % 2) AS p FROM numbers(10);
"
echo "versions after one INSERT: $(versions "${UNPART}")"
lint_commit "${UNPART}" "00000000000000000001" "[]"

echo "-- the same INSERT again appends a second version with the same rows (not idempotent)"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${UNPART}') SELECT number AS id, 'p' || toString(number % 2) AS p FROM numbers(10);
    SELECT count(), uniqExact(id) FROM deltaLakeLocal('${UNPART}');
"
echo "versions after two INSERTs: $(versions "${UNPART}")"

echo "-- a 0-row INSERT on an unpartitioned table: an empty version, no data file"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${UNPART}') SELECT number AS id, 'p' AS p FROM numbers(10) WHERE 0;
    SELECT count() FROM deltaLakeLocal('${UNPART}');
"
echo "versions after a 0-row INSERT: $(versions "${UNPART}")"
echo "data files: $(find "${UNPART}" -name '*.parquet' | wc -l | tr -d ' ')"

echo "==== partitioned by p (a value with a space exercises the URI encoding of add.path)"
PART="${ROOT}/part"
bootstrap "${PART}" "${SCHEMA}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${PART}') SELECT number AS id, if(number % 2 = 0, 'even part', 'odd') AS p FROM numbers(10);
"
echo "versions after one INSERT: $(versions "${PART}")"
lint_commit "${PART}" "00000000000000000001" "['p']"

echo "-- a 0-row INSERT on a partitioned table: no version at all"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${PART}') SELECT number AS id, 'p' AS p FROM numbers(10) WHERE 0;
    SELECT count() FROM deltaLakeLocal('${PART}');
"
echo "versions after a 0-row INSERT: $(versions "${PART}")"
echo "data files: $(find "${PART}" -name '*.parquet' | wc -l | tr -d ' ')"
