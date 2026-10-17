#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
#
# Regression test: `geospatial_statistics.bbox` row-group pruning (the native Parquet geo-stats
# fallback used when a file has no `covering.bbox`) must still work after an Iceberg
# `RENAME COLUMN` on the geometry column.
#
# `rowGroupFailsSpatialFilters` (the `geospatial_statistics.bbox` fallback) matches
# `SpatialFilter::geometry_column_name` - a query-side name - directly against
# `primitive_columns[i].name`, which is a raw/file-side name whenever a per-file
# `column_mapper` is in play (data-lake schema evolution). Without translating the query-side
# name to the raw name first, a renamed geometry column with only `geospatial_statistics.bbox`
# (no `covering.bbox`) silently loses pruning.
#
# Without the fix: `ParquetPrunedRowGroups` is absent (no pruning) even though the query filter
# is disjoint from one of the file's two row groups.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table captures its format settings when it is created, so the setting randomizer turning
# `input_format_parquet_spatial_filter_push_down` off for `CREATE TABLE` disables pruning for good -
# a pin on the query under test cannot bring it back. Pin it for every query in the script instead.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_repeated_settings --input_format_parquet_spatial_filter_push_down 1"

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_geo_evol_geostats"
TEST_TABLE="t_ice_geo_evol_geostats"

rm -rf "${ICEBERG_PATH}"

# Same physical schema as the fixture used by 04512/04513 (id, geometry, and the four scalar
# columns), so field-id assignment (1..6, in declaration order) matches the fixture's
# `PARQUET:field_id` values. The bbox_* columns aren't used by this test - the fixture's "geo"
# metadata has no `covering` entry, only a native `geospatial_statistics.bbox` per row group.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE}
        (id Int32, geometry Point, bbox_xmin Float64, bbox_ymin Float64, bbox_xmax Float64, bbox_ymax Float64)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO ${TEST_TABLE} VALUES (0, (0, 0), 0, 0, 0, 0);
"

# Bump the schema by renaming the geometry column. Existing data files still carry `geometry`
# under their own (now stale) schema id; the current snapshot has the same field_id under the
# new name `geom_renamed`.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TEST_TABLE} RENAME COLUMN geometry TO geom_renamed;
"

# Replace the just-inserted (throwaway) data file with a hand-crafted fixture: 2 row groups
# (south Texas ids 1,2; north Texas ids 3,4), real `geospatial_statistics.bbox` metadata on the
# geometry column and no `covering.bbox` at all.
DATA_FILE=$(find "${ICEBERG_PATH}/data" -name '*.parquet' | head -n 1)
cp "$CUR_DIR/data_parquet/04514_geo_pruning_iceberg_geostats.parquet" "${DATA_FILE}"

echo "=== all rows ==="
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TEST_TABLE} ORDER BY id"

echo "=== south texas ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT id FROM ${TEST_TABLE}
    WHERE pointInPolygon(geom_renamed, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    ORDER BY id"

# The south Texas filter is disjoint from the north Texas row group: with the fix in place this
# must prune exactly 1 row group via the `geospatial_statistics.bbox` fallback. Without the fix,
# the renamed geometry column is looked up under the wrong name and pruning silently never
# engages.
echo "=== pruned_row_groups (expect 1) ==="
${CLICKHOUSE_CLIENT} --print-profile-events --query "
    SELECT id FROM ${TEST_TABLE}
    WHERE pointInPolygon(geom_renamed, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
