#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
#
# Regression test: GeoParquet `covering.bbox` row-group pruning must still work
# after an Iceberg `RENAME COLUMN` on the geometry column.
#
# When an Iceberg table has gone through schema evolution, reads of its
# existing data files go through the schema-changed branch of
# `StorageObjectStorageSource`, which swaps `FormatFilterInfo::column_mapper`
# for a per-file `ColumnMapper` scoped to the schema the file was written
# under (`field_id -> that file's OWN old column name`). But the spatial
# filter's `geometry_column_name` (extracted from the filter DAG, see
# `GeoFilter.cpp`) is always the CURRENT/query-side column name. Resolving
# that name back to a `field_id` (needed to find the matching GeoParquet
# `covering.bbox` metadata) with the per-file mapper builds a self-referential,
# useless map on any renamed column, so pruning silently never engages.
#
# Without the fix: `ParquetPrunedRowGroups` is absent (no pruning) even though
# the query filter is disjoint from one of the file's two row groups.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table captures its format settings when it is created, so the setting randomizer turning
# `input_format_parquet_spatial_filter_push_down` off for `CREATE TABLE` disables pruning for good -
# a pin on the query under test cannot bring it back. Pin it for every query in the script instead.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_repeated_settings --input_format_parquet_spatial_filter_push_down 1"

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_geo_evol"
TEST_TABLE="t_ice_geo_evol"

rm -rf "${ICEBERG_PATH}"

# Create an Iceberg table whose geometry column will be renamed, matching the
# schema of the fixture below: id Int32, geometry (WKB) String, and
# bbox_xmin/ymin/xmax/ymax Float64 referenced by the GeoParquet
# `covering.bbox` metadata. Column declaration order determines Iceberg's
# field-id assignment (1..6 in order), which must match the fixture's
# `PARQUET:field_id` values.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE}
        (id Int32, geometry Point, bbox_xmin Float64, bbox_ymin Float64, bbox_xmax Float64, bbox_ymax Float64)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO ${TEST_TABLE} VALUES (0, (0, 0), 0, 0, 0, 0);
"

# Bump the schema by renaming the geometry column. Existing data files still
# use the old name `geometry` under their own (now stale) schema id; the
# current snapshot has the same field_id under the new name `geom_renamed`.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TEST_TABLE} RENAME COLUMN geometry TO geom_renamed;
"

# Replace the just-inserted (throwaway) data file with a hand-crafted fixture
# that has the same field_ids, 2 row groups (south Texas ids 1,2; north Texas
# ids 3,4), and real `covering.bbox` GeoParquet metadata copied from
# `04059_geo_spatial_pruning.parquet`.
DATA_FILE=$(find "${ICEBERG_PATH}/data" -name '*.parquet' | head -n 1)
cp "$CUR_DIR/data_parquet/04512_geo_pruning_iceberg.parquet" "${DATA_FILE}"

echo "=== all rows ==="
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TEST_TABLE} ORDER BY id"

echo "=== south texas ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT id FROM ${TEST_TABLE}
    WHERE pointInPolygon(geom_renamed, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    ORDER BY id"

# The south Texas filter is disjoint from the north Texas row group: with the
# fix in place this must prune exactly 1 row group. Without the fix, no
# pruning happens on the renamed column at all.
echo "=== pruned_row_groups (expect 1) ==="
${CLICKHOUSE_CLIENT} --print-profile-events --query "
    SELECT id FROM ${TEST_TABLE}
    WHERE pointInPolygon(geom_renamed, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
