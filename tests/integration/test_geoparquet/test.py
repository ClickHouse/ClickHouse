import os
import time

import pyarrow.parquet as pq
import pytest
import geopandas as gpd

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
path_to_userfiles = "/var/lib/clickhouse/user_files/"
node = cluster.add_instance("node", external_dirs=[path_to_userfiles])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_page_index(file_path):
    metadata = pq.read_metadata(file_path)
    assert (
        metadata
    ), "pyarrow.parquet library can't read parquet file written by Clickhouse"
    return metadata.row_group(0).column(0).has_offset_index


def delete_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.mark.parametrize(
    "query, expected_result",
    [
        (
            "DROP TABLE IF EXISTS geom1;"
            "CREATE TABLE IF NOT EXISTS geom1 (point Point) ENGINE = Memory();"
            "INSERT INTO geom1 VALUES((10, 20));"
            "SELECT * FROM geom1 ORDER BY point INTO OUTFILE '{file_name}' FORMAT Parquet;",
            'POINT (10 20)',
        ),
        (
            "DROP TABLE IF EXISTS geom2;"
            "CREATE TABLE IF NOT EXISTS geom2 (point LineString) ENGINE = Memory();"
            "INSERT INTO geom2 VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);"
            "SELECT * FROM geom2 ORDER BY point INTO OUTFILE '{file_name}' FORMAT Parquet;",
            'LINESTRING (0 0, 10 0, 10 10, 0 10)',
        ),
        (
            "DROP TABLE IF EXISTS geom3;"
            "CREATE TABLE IF NOT EXISTS geom3 (point Polygon) ENGINE = Memory();"
            "INSERT INTO geom3 VALUES([[(20, 20), (50, 20), (50, 50), (20, 50), (20, 20)], [(30, 30), (50, 50), (50, 30), (30, 30)]]);"
            "SELECT * FROM geom3 ORDER BY point INTO OUTFILE '{file_name}' FORMAT Parquet;",
            'POLYGON ((20 20, 50 20, 50 50, 20 50, 20 20), (30 30, 50 50, 50 30, 30 30))',
        ),
        (
            "DROP TABLE IF EXISTS geom4;"
            "CREATE TABLE IF NOT EXISTS geom4 (point MultiLineString) ENGINE = Memory();"
            "INSERT INTO geom4 VALUES([[(0, 0), (10, 0)], [(10, 10), (0, 10)], []]);"
            "SELECT * FROM geom4 ORDER BY point INTO OUTFILE '{file_name}' FORMAT Parquet;",
            'MULTILINESTRING ((0 0, 10 0), (10 10, 0 10), EMPTY)',
        ),
        (
            "DROP TABLE IF EXISTS geom5;"
            "CREATE TABLE IF NOT EXISTS geom5 (point MultiPolygon) ENGINE = Memory();"
            "INSERT INTO geom5 VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]], [[(20, 20), (50, 20), (50, 50), (20, 50), (20, 20)],[(30, 30), (50, 50), (50, 30), (30, 30)]]]);"
            "SELECT * FROM geom5 ORDER BY point INTO OUTFILE '{file_name}' FORMAT Parquet;",
            'MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 50 20, 50 50, 20 50, 20 20), (30 30, 50 50, 50 30, 30 30)))',
        )
    ],
)
def test_compatibiltity_with_geopandas(query, expected_result, start_cluster):
    file_name = f"export{time.time()}.parquet"
    query = query.format(file_name=file_name)
    delete_if_exists(file_name)
    assert node.query(query) == ""
    result = gpd.read_parquet(file_name)
    assert str(result['point'][0]) == expected_result
    delete_if_exists(file_name)
