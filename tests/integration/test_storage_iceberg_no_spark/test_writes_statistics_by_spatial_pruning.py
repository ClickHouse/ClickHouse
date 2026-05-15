import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    create_iceberg_table,
    get_uuid_str,
)

SOUTH_TX = "[(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)]"
NORTH_TX = "[(-99., 33.), (-96., 33.), (-96., 36.), (-99., 36.), (-99., 33.)]"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_statistics_by_spatial_pruning(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_statistics_by_spatial_pruning_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    # geometry_bbox_{xmin,ymin,xmax,ymax} follow the flat-column naming convention
    # expected by ManifestFilesPruner when looking for covering.bbox columns for a
    # geometry column named "geometry".
    schema = """
    (id Int32,
    geometry Tuple(Float64, Float64),
    geometry_bbox_xmin Float64,
    geometry_bbox_ymin Float64,
    geometry_bbox_xmax Float64,
    geometry_bbox_ymax Float64)
    """
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        schema,
        format_version,
    )

    # Each INSERT produces a separate Parquet file with its own column statistics.
    # File 1: one point in south Texas (lat ~31).
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, (-98.0, 31.5), -98.0, 31.5, -98.0, 31.5);"
    )
    # File 2: one point in north Texas (lat ~34).
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2, (-97.5, 34.5), -97.5, 34.5, -97.5, 34.5);"
    )

    settings_off = {"use_iceberg_partition_pruning": 0}
    settings_on = {"use_iceberg_partition_pruning": 1}

    def check(select_expression):
        return check_validity_and_get_prunned_files_general(
            instance,
            TABLE_NAME,
            settings_off,
            settings_on,
            "IcebergMinMaxIndexPrunedFiles",
            select_expression,
        )

    # South Texas filter: only file 1 overlaps; file 2 (north Texas) must be pruned.
    assert (
        check(
            f"SELECT id FROM {TABLE_NAME} WHERE pointInPolygon(geometry, {SOUTH_TX}) ORDER BY id"
        )
        == 1
    )

    # North Texas filter: only file 2 overlaps; file 1 (south Texas) must be pruned.
    assert (
        check(
            f"SELECT id FROM {TABLE_NAME} WHERE pointInPolygon(geometry, {NORTH_TX}) ORDER BY id"
        )
        == 1
    )

    # OR-safety: the spatial predicate is joined by OR with a non-spatial predicate that
    # matches a row in file 2 (north Texas). Pruning file 2 would incorrectly drop id=2,
    # so no file should be pruned.
    assert (
        check(
            f"SELECT id FROM {TABLE_NAME} WHERE pointInPolygon(geometry, {SOUTH_TX}) OR id = 2 ORDER BY id"
        )
        == 0
    )
