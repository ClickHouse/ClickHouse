#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
#
# Regression test: GeoParquet `covering.bbox` row-group pruning must still work
# after an Iceberg `RENAME COLUMN` on a `covering.bbox` sub-column (not just the
# geometry column itself, see `04512_parquet_geo_pruning_iceberg_renamed_column.sh`).
#
# `covering.bbox` metadata (`xmin_column`/`ymin_column`/`xmax_column`/`ymax_column`)
# always names the raw parquet-side columns of the file it came from. Once any of
# those columns has gone through an Iceberg schema-evolution rename, resolving
# them to/from the query-side name used by `extended_sample_block` and
# `primitive_columns` requires the same `ColumnMapper::makeMapping` translation
# as the geometry column - without it, the injected bbox columns are looked up
# under the wrong name and `covering.bbox` pruning silently never engages.
#
# Without the fix: `ParquetPrunedRowGroups` is absent (no pruning) even though
# the query filter is disjoint from one of the file's two row groups.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_geo_evol_bbox"
TEST_TABLE="t_ice_geo_evol_bbox"

rm -rf "${ICEBERG_PATH}"

# Same physical schema as the fixture used by 04512 (id, geometry, and the four
# `covering.bbox` scalar columns), so field-id assignment (1..6, in declaration
# order) matches the fixture's `PARQUET:field_id` values.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE}
        (id Int32, geometry Point, bbox_xmin Float64, bbox_ymin Float64, bbox_xmax Float64, bbox_ymax Float64)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO ${TEST_TABLE} VALUES (0, (0, 0), 0, 0, 0, 0);
"

# Bump the schema by renaming one of the `covering.bbox` sub-columns. Existing
# data files still carry `bbox_xmin` under their own (now stale) schema id; the
# current snapshot has the same field_id under the new name `bbox_xmin_renamed`.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TEST_TABLE} RENAME COLUMN bbox_xmin TO bbox_xmin_renamed;
"

# Replace the just-inserted (throwaway) data file with the same hand-crafted
# fixture used by 04512: 2 row groups (south Texas ids 1,2; north Texas ids
# 3,4), real `covering.bbox` GeoParquet metadata pointing at its own raw
# `bbox_xmin`/etc columns.
DATA_FILE=$(find "${ICEBERG_PATH}/data" -name '*.parquet' | head -n 1)
cp "$CUR_DIR/data_parquet/04512_geo_pruning_iceberg.parquet" "${DATA_FILE}"

echo "=== all rows ==="
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TEST_TABLE} ORDER BY id"

echo "=== south texas ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT id FROM ${TEST_TABLE}
    WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    ORDER BY id"

# The south Texas filter is disjoint from the north Texas row group: with the
# fix in place this must prune exactly 1 row group via `covering.bbox` stats.
# Without the fix, the renamed bbox column is looked up under the wrong name
# and pruning silently never engages.
echo "=== pruned_row_groups (expect 1) ==="
${CLICKHOUSE_CLIENT} --print-profile-events --query "
    SELECT id FROM ${TEST_TABLE}
    WHERE pointInPolygon(geometry, [(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)])
    ORDER BY id" 2>&1 | grep 'ParquetPrunedRowGroups' | sed 's/^.*] //'

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
