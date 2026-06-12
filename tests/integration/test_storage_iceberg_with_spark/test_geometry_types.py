import glob
import json
import os

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)
from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.wkb import dumps as wkb_dumps

from helpers.test_tools import TSV


ICEBERG_WAREHOUSE = "/var/lib/clickhouse/user_files/iceberg_data"


def _wkb_hex(geom) -> str:
    return wkb_dumps(geom).hex().upper()


_WKB_POINT = _wkb_hex(Point(1.0, 2.0))
_WKB_LINESTRING = _wkb_hex(LineString([(0, 0), (1, 1), (2, 2)]))
_WKB_POLYGON = _wkb_hex(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))
_WKB_MULTILINESTRING = _wkb_hex(MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]))
_WKB_MULTIPOLYGON = _wkb_hex(
    MultiPolygon(
        [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 2)]),
        ]
    )
)


def _patch_column_type(table_path: str, column_name: str, new_type: str) -> None:
    pattern = os.path.join(table_path, "metadata", "*.metadata.json")
    files = sorted(glob.glob(pattern))
    assert files, f"No metadata JSON found under {table_path}/metadata/"
    path = files[-1]

    with open(path) as f:
        meta = json.load(f)

    def _patch(fields):
        for field in fields:
            if field["name"] == column_name:
                field["type"] = new_type

    for schema in meta.get("schemas", []):
        _patch(schema.get("fields", []))
    _patch(meta.get("schema", {}).get("fields", []))

    with open(path, "w") as f:
        json.dump(meta, f)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_geometry_type(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_geometry_type_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, geom BINARY, geog BINARY)
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.row-group-size-bytes'='104850')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, unhex('{_WKB_POINT}'),           unhex('{_WKB_POINT}')),
        (2, unhex('{_WKB_LINESTRING}'),       unhex('{_WKB_LINESTRING}')),
        (3, unhex('{_WKB_POLYGON}'),          unhex('{_WKB_POLYGON}')),
        (4, unhex('{_WKB_MULTILINESTRING}'),  unhex('{_WKB_MULTILINESTRING}')),
        (5, unhex('{_WKB_MULTIPOLYGON}'),     unhex('{_WKB_MULTIPOLYGON}'))
        """
    )

    local_table_path = f"{ICEBERG_WAREHOUSE}/default/{TABLE_NAME}"
    _patch_column_type(local_table_path, "geom", "geometry(srid:4326)")
    _patch_column_type(local_table_path, "geog", "geography")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark,
        settings={"allow_experimental_geo_types_in_iceberg": 1},
    )

    table_function_expr = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    geo_settings = {"allow_experimental_geo_types_in_iceberg": 1}

    assert instance.query(
        f"DESCRIBE {table_function_expr} FORMAT TSV", settings=geo_settings
    ) == TSV(
        [
            ["id", "Nullable(Int32)"],
            ["geom", "Geometry"],
            ["geog", "Geometry"],
        ]
    )

    result = instance.query(
        f"SELECT id, variantType(geom), variantType(geog)"
        f" FROM {table_function_expr} ORDER BY id FORMAT TSV",
        settings=geo_settings,
    )
    assert result.strip() == (
        "1\tPoint\tPoint\n"
        "2\tLineString\tLineString\n"
        "3\tPolygon\tPolygon\n"
        "4\tMultiLineString\tMultiLineString\n"
        "5\tMultiPolygon\tMultiPolygon"
    )

    assert (
        instance.query(
            f"SELECT variantElement(geom, 'Point')"
            f" FROM {table_function_expr} WHERE id = 1 FORMAT TSV",
            settings=geo_settings,
        ).strip()
        == "(1,2)"
    )
    assert (
        instance.query(
            f"SELECT length(variantElement(geom, 'LineString'))"
            f" FROM {table_function_expr} WHERE id = 2 FORMAT TSV",
            settings=geo_settings,
        ).strip()
        == "3"
    )
    assert (
        instance.query(
            f"SELECT length(variantElement(geom, 'Polygon'))"
            f" FROM {table_function_expr} WHERE id = 3 FORMAT TSV",
            settings=geo_settings,
        ).strip()
        == "1"
    )
    assert (
        instance.query(
            f"SELECT length(variantElement(geom, 'MultiLineString'))"
            f" FROM {table_function_expr} WHERE id = 4 FORMAT TSV",
            settings=geo_settings,
        ).strip()
        == "2"
    )
    assert (
        instance.query(
            f"SELECT length(variantElement(geom, 'MultiPolygon'))"
            f" FROM {table_function_expr} WHERE id = 5 FORMAT TSV",
            settings=geo_settings,
        ).strip()
        == "2"
    )


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_geometry_write(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_geometry_write_" + storage_type + "_" + get_uuid_str()

    geo_settings = {"allow_experimental_geo_types_in_iceberg": 1}

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        schema="(id Int32, geom Geometry)",
        format_version=2,
        settings=geo_settings,
    )

    instance.query(
        f"""
        INSERT INTO {TABLE_NAME}
        SELECT 1, readWkt('POINT(1 2)') UNION ALL
        SELECT 2, readWkt('LINESTRING(0 0, 1 1, 2 2)') UNION ALL
        SELECT 3, readWkt('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') UNION ALL
        SELECT 4, readWkt('MULTILINESTRING((0 0, 1 1),(2 2, 3 3))') UNION ALL
        SELECT 5, readWkt('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)),((2 2, 3 2, 3 3, 2 2)))')
        """,
        settings={"allow_insert_into_iceberg": 1, **geo_settings},
    )

    result = instance.query(
        f"SELECT id, variantType(geom) FROM {TABLE_NAME} ORDER BY id FORMAT TSV",
        settings=geo_settings,
    )
    assert result.strip() == (
        "1\tPoint\n"
        "2\tLineString\n"
        "3\tPolygon\n"
        "4\tMultiLineString\n"
        "5\tMultiPolygon"
    )

    assert (
        instance.query(
            f"SELECT variantElement(geom, 'Point') FROM {TABLE_NAME} ORDER BY id LIMIT 1 FORMAT TSV",
            settings=geo_settings,
        ).strip()
        == "(1,2)"
    )
