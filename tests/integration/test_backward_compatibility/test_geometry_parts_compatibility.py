"""
`MultiPoint` was appended to the variants of `Geometry` after 26.7, so a part with a `Geometry` column
written by an older version has no streams for it. Such parts must stay readable, mergeable and mutable.
"""

import pytest

from helpers.cluster import ClickHouseCluster

# The last release whose `Geometry` has no `MultiPoint`.
OLD_VERSION = "26.6"

WIDE = "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
COMPACT = "min_bytes_for_wide_part = '200G', min_rows_for_wide_part = 1000000"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=OLD_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

QUERIES = {
    "t": "SELECT id, g, variantType(g) FROM t ORDER BY id",
    "t_compact": "SELECT id, g, variantType(g) FROM t_compact ORDER BY id",
    "t_arr": "SELECT id, a, arrayMap(x -> variantType(x), a) FROM t_arr ORDER BY id",
    "t_json": "SELECT id, j, j.p, variantType(j.p) FROM t_json ORDER BY id",
    "t_nested": "SELECT id, n.g, arrayMap(x -> variantType(x), n.g) FROM t_nested ORDER BY id",
    "t_default": "SELECT id, g, variantType(g) FROM t_default ORDER BY id",
    "t_geo_only": "SELECT g, variantType(g) FROM t_geo_only ORDER BY variantType(g)",
}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def parts_substreams(table):
    paths = node.query(
        f"SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '{table}' AND active"
    ).split()
    assert paths, table
    return [
        node.exec_in_container(["cat", f"{path}columns_substreams.txt"])
        for path in paths
    ]


def test_geometry_parts_without_multipoint(start_cluster):
    node.query(
        f"CREATE TABLE t (id UInt64, v UInt64, g Geometry) ENGINE = MergeTree ORDER BY id SETTINGS {WIDE}"
    )
    node.query(
        "INSERT INTO t VALUES (1, 1, (1., 2.)::Point), (2, 2, [(1., 2.), (3., 4.)]::LineString), (3, 3, NULL)"
    )
    node.query("""
        INSERT INTO t VALUES (4, 4, [(0., 0.), (1., 0.), (1., 1.)]::Ring), (5, 5, [[(0., 0.), (2., 0.), (2., 2.)]]::Polygon),
            (6, 6, [[(1., 1.), (2., 2.)], [(3., 3.), (4., 4.)]]::MultiLineString), (7, 7, [[[(0., 0.), (5., 0.), (5., 5.)]]]::MultiPolygon)
        """)

    node.query(
        f"CREATE TABLE t_compact (id UInt64, g Geometry) ENGINE = MergeTree ORDER BY id SETTINGS {COMPACT}"
    )
    node.query(
        "INSERT INTO t_compact VALUES (1, (1., 2.)::Point), (2, [(1., 2.), (3., 4.)]::LineString), (3, NULL)"
    )

    node.query(
        f"CREATE TABLE t_arr (id UInt64, a Array(Geometry)) ENGINE = MergeTree ORDER BY id SETTINGS {WIDE}"
    )
    node.query("""
        INSERT INTO t_arr VALUES (1, [(1., 2.)::Point, [(1., 2.), (3., 4.)]::LineString]), (2, []),
            (3, [NULL, [(0., 0.), (1., 0.), (1., 1.)]::Ring])
        """)

    node.query(
        f"CREATE TABLE t_json (id UInt64, j JSON(p Geometry)) ENGINE = MergeTree ORDER BY id SETTINGS {WIDE}"
    )
    node.query("""INSERT INTO t_json SELECT 1, '{"p": [1.5, 2.5]}'""")
    node.query("""INSERT INTO t_json SELECT 2, '{"p": [[1, 2], [3, 4]], "q": 7}'""")
    node.query("""INSERT INTO t_json SELECT 3, '{"q": 1}'""")

    node.query(
        f"CREATE TABLE t_nested (id UInt64, n Nested(g Geometry)) ENGINE = MergeTree ORDER BY id SETTINGS {WIDE}"
    )
    node.query("""
        INSERT INTO t_nested VALUES (1, [(1., 2.)::Point, [(1., 2.), (3., 4.)]::LineString]), (2, []),
            (3, [NULL, [(0., 0.), (1., 0.), (1., 1.)]::Ring])
        """)

    node.query(
        f"CREATE TABLE t_default (id UInt64, g Geometry DEFAULT (0., 0.)::Point) ENGINE = MergeTree ORDER BY id SETTINGS {WIDE}"
    )
    node.query(
        "INSERT INTO t_default VALUES (1, [(1., 2.), (3., 4.)]::LineString), (2, [[(0., 0.), (2., 0.), (2., 2.)]]::Polygon)"
    )

    node.query(
        f"CREATE TABLE t_geo_only (g Geometry) ENGINE = MergeTree ORDER BY tuple() SETTINGS {WIDE}"
    )
    node.query(
        "INSERT INTO t_geo_only VALUES ((1., 2.)::Point), ([(1., 2.), (3., 4.)]::LineString)"
    )

    expected = {table: node.query(query) for table, query in QUERIES.items()}

    node.restart_with_latest_version()

    for table in QUERIES:
        for substreams in parts_substreams(table):
            assert (
                "variant_discr" in substreams and "MultiPoint" not in substreams
            ), substreams

    assert node.query("SELECT groupUniqArray(g.MultiPoint) FROM t") == "[[]]\n"
    assert node.query("SELECT groupUniqArray(g.MultiPoint.size0) FROM t") == "[0]\n"
    assert node.query("SELECT groupUniqArray(g.MultiPoint) FROM t_compact") == "[[]]\n"
    assert (
        node.query("SELECT a.MultiPoint FROM t_arr ORDER BY id")
        == "[[],[]]\n[]\n[[],[]]\n"
    )
    assert node.query("SELECT groupUniqArray(j.p.MultiPoint) FROM t_json") == "[[]]\n"
    assert (
        node.query("SELECT n.g.MultiPoint FROM t_nested ORDER BY id")
        == "[[],[]]\n[]\n[[],[]]\n"
    )

    for table, query in QUERIES.items():
        assert node.query(query) == expected[table], table
    assert node.query("SELECT id FROM t WHERE variantType(g) = 'LineString'") == "2\n"

    assert node.query("SELECT uniqExact(_part), count() FROM t_geo_only") == "1\t2\n"
    node.query("ALTER TABLE t_default MATERIALIZE COLUMN g SETTINGS mutations_sync = 2")
    assert node.query(QUERIES["t_default"]) == expected["t_default"]

    node.query(
        "ALTER TABLE t UPDATE g = [(5., 6.), (7., 8.)]::MultiPoint WHERE id = 3 SETTINGS mutations_sync = 2"
    )
    assert "3\t\\N\tNone\n" in expected["t"]
    expected["t"] = expected["t"].replace("3\t\\N\tNone\n", "3\t[(5,6),(7,8)]\tMultiPoint\n")
    assert node.query(QUERIES["t"]) == expected["t"]
    part = node.query("SELECT _part FROM t WHERE id = 3").strip()
    path = node.query(
        f"SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't' AND name = '{part}'"
    ).strip()
    assert "MultiPoint" in node.exec_in_container(["cat", f"{path}columns_substreams.txt"])

    node.query("INSERT INTO t VALUES (8, 8, [(7., 8.), (9., 10.)]::MultiPoint)")
    for table in QUERIES:
        node.query(f"OPTIMIZE TABLE {table} FINAL")
        for substreams in parts_substreams(table):
            assert "MultiPoint" in substreams, substreams

    assert node.query(QUERIES["t"]) == expected["t"] + "8\t[(7,8),(9,10)]\tMultiPoint\n"
    for table in ["t_compact", "t_arr", "t_json", "t_nested", "t_default", "t_geo_only"]:
        assert node.query(QUERIES[table]) == expected[table], table
