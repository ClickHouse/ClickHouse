#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires Iceberg (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows
# Every `metadata.json` an Iceberg writer produces lists only the previous files in `metadata-log`
# and omits `parent-snapshot-id` on a snapshot without a parent (#118812).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Background compaction (driven by a member flag in the cloud build) and the metadata files cache
# would rewrite or hide the files each step writes; compaction is enabled only for explicit OPTIMIZE.
CH_BASE="${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 --allow_insert_into_iceberg 1 --mutations_sync 2"
CH="${CH_BASE} --allow_experimental_iceberg_compaction 0"
CH_OPTIMIZE="${CH_BASE} --allow_experimental_iceberg_compaction 1"

# One row per metadata file: the basenames its `metadata-log` lists, whether every entry carries the
# `last-updated-ms` and the location-based path of the file it names, whether the entries' times are distinct,
# and which snapshots have a parent.
dump()
{
    local src="$1"
    ${CH} -q "
        WITH (SELECT mapFromArrays(groupArray(_file), groupArray(JSONExtractInt(json, 'last-updated-ms'))) FROM ${src}) AS updated_ms
        SELECT
            _file,
            arrayMap(e -> splitByChar('/', JSONExtractString(e, 'metadata-file'))[-1], JSONExtractArrayRaw(json, 'metadata-log')) AS log,
            arrayAll(e -> JSONExtractInt(e, 'timestamp-ms') = updated_ms[splitByChar('/', JSONExtractString(e, 'metadata-file'))[-1]],
                JSONExtractArrayRaw(json, 'metadata-log')) AS ts_ok,
            length(arrayDistinct(arrayMap(e -> JSONExtractInt(e, 'timestamp-ms'), JSONExtractArrayRaw(json, 'metadata-log'))))
                = length(JSONExtractArrayRaw(json, 'metadata-log')) AS ts_distinct,
            arrayAll(e -> JSONExtractString(e, 'metadata-file') = concat(trim(TRAILING '/' FROM JSONExtractString(json, 'location')), '/metadata/',
                splitByChar('/', JSONExtractString(e, 'metadata-file'))[-1]), JSONExtractArrayRaw(json, 'metadata-log')) AS path_ok,
            arrayMap(s -> JSONHas(s, 'parent-snapshot-id'), JSONExtractArrayRaw(json, 'snapshots')) AS has_parent
        FROM ${src}
        ORDER BY toUInt32(splitByChar('.', substring(_file, 2))[1])"
}

# Snapshots whose parent is not a snapshot of the table, as `system.iceberg_history` reports them.
root_snapshots()
{
    ${CH} -q "
        SELECT $1 FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '$2'
            AND parent_id NOT IN (SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '$2')"
}

orphan_metadata_files()
{
    sleep 2
    ${CH} --allow_iceberg_remove_orphan_files 1 --iceberg_orphan_files_older_than_seconds 0 \
        --iceberg_orphan_files_min_retention_seconds 0 \
        -q "ALTER TABLE $1 EXECUTE remove_orphan_files(dry_run = 1)" | grep -F deleted_metadata_files_count
}

echo 'Part A: every writer, IcebergLocal'
A_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}_a"
A_PATH="${USER_FILES_PATH}/${A_DIR}/"
${CH} -q "CREATE TABLE ta (id Int64, p Int64) ENGINE = IcebergLocal('${A_PATH}', 'Parquet') PARTITION BY p SETTINGS iceberg_format_version = 2"
${CH} -q "INSERT INTO ta VALUES (1, 0), (2, 1)"
${CH} -q "INSERT INTO ta VALUES (3, 0), (4, 1)"
root_snapshots parent_id ta
ROOT_ID=$(root_snapshots snapshot_id ta)
${CH} -q "ALTER TABLE ta ADD COLUMN c Nullable(Int64)"
${CH} -q "ALTER TABLE ta DELETE WHERE id = 1"
${CH} -q "ALTER TABLE ta DROP PARTITION 0"
${CH} --allow_experimental_expire_snapshots 1 -q "ALTER TABLE ta EXECUTE expire_snapshots(snapshot_ids = [${ROOT_ID}])" > /dev/null
${CH_OPTIMIZE} -q "OPTIMIZE TABLE ta MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1"
dump "file('${A_DIR}/metadata/*.metadata.json', 'JSONAsString')"
${CH} -q "SELECT * FROM ta ORDER BY id"
orphan_metadata_files ta

echo 'Part B: IcebergS3'
B_DIR="05366_iceberg_metadata_log/${CLICKHOUSE_TEST_UNIQUE_NAME}/b"
${CH} -q "CREATE TABLE tb (id Int64) ENGINE = IcebergS3(s3_conn, filename = '${B_DIR}') SETTINGS iceberg_format_version = 2"
${CH} -q "INSERT INTO tb VALUES (1)"
${CH} -q "INSERT INTO tb VALUES (2)"
${CH} -q "ALTER TABLE tb ADD COLUMN c Nullable(Int64)"
${CH} -q "INSERT INTO tb VALUES (3, 3)"
dump "s3(s3_conn, filename = '${B_DIR}/metadata/*.metadata.json', format = 'JSONAsString')"
orphan_metadata_files tb

echo 'Part C: OPTIMIZE full rewrite'
C_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}_c"
C_PATH="${USER_FILES_PATH}/${C_DIR}/"
${CH} -q "CREATE TABLE tc (id Int64) ENGINE = IcebergLocal('${C_PATH}', 'Parquet') SETTINGS iceberg_format_version = 2"
${CH} -q "INSERT INTO tc VALUES (1)"
${CH} -q "INSERT INTO tc VALUES (2)"
# The full rewrite runs only when there are position deletes.
${CH} -q "ALTER TABLE tc DELETE WHERE id = 1"
${CH_OPTIMIZE} -q "OPTIMIZE TABLE tc"
# Cloud can return from `OPTIMIZE TABLE` before the new metadata becomes visible.
for ((attempt = 0; attempt < 30; ++attempt)); do
    test -f "${C_PATH}metadata/v5.metadata.json" && break
    sleep 1
done
${CH} -q "
    SELECT
        arrayMap(e -> splitByChar('/', JSONExtractString(e, 'metadata-file'))[-1], JSONExtractArrayRaw(json, 'metadata-log')) AS log,
        arrayMap(s -> JSONHas(s, 'parent-snapshot-id'), JSONExtractArrayRaw(json, 'snapshots')) AS has_parent
    FROM file('${C_DIR}/metadata/v5.metadata.json', 'JSONAsString')"
${CH} -q "SELECT * FROM tc ORDER BY id"

${CH} -q "DROP TABLE ta SYNC; DROP TABLE tb SYNC; DROP TABLE tc SYNC"
rm -rf "${A_PATH}" "${C_PATH}"
