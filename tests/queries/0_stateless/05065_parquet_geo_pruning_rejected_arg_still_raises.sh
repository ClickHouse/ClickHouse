#!/usr/bin/env bash
# GeoParquet row-group pruning shares the bbox extractor in `GeoBbox.h` with the `spatial_bbox` skip
# index, and it prunes harder: `Parquet::ReadManager::read` returns EOF once every row group is gone,
# rather than handing the pipeline an empty block. So the question is whether a sibling conjunct whose
# argument the predicate rejects still raises once pruning has removed everything.
#
# It does. `callOnTwoGeometryDataTypes` runs while the pipeline is built, on the header, so the
# rejection is settled before the reader is ever asked for a row group. The first query below shows
# every row group pruned; the rest show the exception surviving that pruning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_USER_FILES:?}/${CLICKHOUSE_DATABASE}_geo_pruning.parquet"
cp "$CUR_DIR/data_parquet/04512_geo_pruning_iceberg.parquet" "$DATA_FILE"

FAR_POLYGON="[[(1000., 1000.), (1001., 1000.), (1001., 1001.), (1000., 1000.)]]"

echo "=== every row group pruned ==="
${CLICKHOUSE_CLIENT} --print-profile-events --query "
    SELECT count() FROM file('$(basename "$DATA_FILE")', Parquet)
    WHERE pointInPolygon(geometry, ${FAR_POLYGON})" 2>&1 \
    | grep -E 'ParquetPrunedRowGroups|^[0-9]+$' | sed 's/^.*] //'

# A non-geometry constant, and a geometry kind this predicate refuses at that position. Both are
# rejected on argument types, so both must raise even though nothing is left to read.
for sibling in \
    "polygonsIntersectCartesian(geometry, 1)" \
    "polygonsIntersectCartesian(geometry, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])"
do
    echo "=== sibling: ${sibling} ==="
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM file('$(basename "$DATA_FILE")', Parquet)
        WHERE pointInPolygon(geometry, ${FAR_POLYGON}) AND ${sibling}
        SETTINGS short_circuit_function_evaluation = 'disable'" 2>&1 \
        | grep -om1 'Code: [0-9]*'
done

rm -f "$DATA_FILE"
