#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Compaction reads the latest metadata file itself, while a table object's in-memory schema is only
# refreshed by `updateExternalDynamicMetadataIfExists`, which `InterpreterOptimizeQuery` never
# calls. A table object created before a schema change therefore still names the old columns, and
# `OPTIMIZE` used to rewrite the data files under those names.
#
# The schema change here is a two-column name swap: every old name still exists in the current
# schema, so the write-side field-id validation in `PrepareForWrite` cannot fire, and the rewrite
# committed the two columns' values under each other's field ids without reporting anything.
#
# The rows are spread over several data files and one row is deleted, so compaction has work to do
# and the remapping path runs for every file written before the swap. The position delete file must
# be gone afterwards: reads apply it whether or not compaction ran, so the value assertions alone
# cannot tell a real compaction from a silent no-op.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

table="t_${CLICKHOUSE_DATABASE}"
stale="stale_${CLICKHOUSE_DATABASE}"
table_path="${USER_FILES_PATH}/${table}/"

count_position_deletes()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '${table}' AND content = 'POSITION_DELETE'
    "
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table} (id Int64, x String, y String)
    ENGINE = IcebergLocal('${table_path}', 'Parquet')
"

# One insert sink: each sink commits its own snapshot, so with more than one they contend for the
# same metadata version and the loser of the compare-and-swap logs its lost commit at Error level.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_max_rows_in_data_file=2 \
    --min_insert_block_size_rows=2 --max_insert_block_size=2 --max_block_size=2 --max_insert_threads=1 \
    --query "INSERT INTO ${table} SELECT number, char(number + ascii('a')), char(number + ascii('A')) FROM numbers(6)"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query \
    "ALTER TABLE ${table} DELETE WHERE id = 2"

# A second object over the same Iceberg table, created before the schema change and never queried
# afterwards, so its in-memory schema stays on the pre-swap column names.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${stale} ENGINE = IcebergLocal('${table_path}', 'Parquet')"

# Swap the two column names. The field ids are untouched, so this is a rename, which compaction
# supports, and both names stay resolvable in the current schema.
for rename in "x TO t" "y TO x" "t TO y"; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${table} RENAME COLUMN ${rename}"
done

pd_before=$(count_position_deletes)

# The metadata files cache is bypassed so that compaction reads the post-swap metadata: served from
# the cache it would read the same schema the stale object has, and nothing would be remapped.
optimize_err=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --allow_insert_into_iceberg=1 \
    --use_iceberg_metadata_files_cache=0 --query "OPTIMIZE TABLE ${stale}" 2>&1)

# The cloud build routes `OPTIMIZE` through a different code path and reports a regular user-facing
# exception instead of compacting, so only the open-source build must succeed and compact.
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo "OPTIMIZE accepted"
    echo "position deletes compacted away"
elif [[ -n "${optimize_err}" ]]; then
    echo "FAIL: OPTIMIZE through the stale table object failed: ${optimize_err}"
    echo "position deletes not checked"
else
    echo "OPTIMIZE accepted"
    pd_after=$(count_position_deletes)
    if [[ "${pd_before}" -gt 0 && "${pd_after}" -eq 0 ]]; then
        echo "position deletes compacted away"
    else
        echo "FAIL: position delete file not compacted away: before=${pd_before} after=${pd_after}"
    fi
fi

${CLICKHOUSE_CLIENT} --query "SELECT id, x, y FROM ${table} ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${stale} SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table} SYNC"
rm -rf "${table_path}" 2>/dev/null
