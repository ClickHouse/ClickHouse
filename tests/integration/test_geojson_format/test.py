"""Differential tests for the GeoJSON input/output format against an independent oracle.

ClickHouse's GeoJSON format is a hand-written RFC 7946 parser/serializer. To catch a *consistent*
misinterpretation that a ClickHouse-only round-trip cannot (both sides would agree on the same wrong
answer), each geometry is round-tripped through ClickHouse (GeoJSON -> `Geometry` -> GeoJSON) and the
result is compared against Shapely, which parses GeoJSON with GEOS -- a fully independent codebase.

The corpus is the RFC 7946 Appendix A geometry examples plus adversarial cases (high-precision
coordinates, an antimeridian-crossing line, a 3D position, non-RFC winding, and degenerate shapes).
"""

import json

import pytest
from shapely import equals_exact, from_geojson, get_coordinates

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _feature_collection(geometry):
    """Wrap a single geometry in a one-feature FeatureCollection document."""
    return json.dumps(
        {
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "geometry": geometry, "properties": {}}],
        }
    )


def ch_roundtrip(geometry, settings=""):
    """Read a geometry through the GeoJSON input format and write it back out, returning the
    geometry object ClickHouse emitted (GeoJSON -> `Geometry` -> GeoJSON)."""
    document = _feature_collection(geometry).replace("'", "''")
    query = f"SELECT geometry FROM format(GeoJSON, '{document}') FORMAT GeoJSON {settings}"
    emitted = json.loads(node.query(query))
    return emitted["features"][0]["geometry"]


# RFC 7946 Appendix A examples that ClickHouse's `Geometry` type can represent, plus adversarial
# coordinate cases. `MultiPoint` and `GeometryCollection` are covered separately (they are stored as
# NULL, so they cannot round-trip through a geometry value) in the stateless tests.
ROUNDTRIP_GEOMETRIES = {
    "point": {"type": "Point", "coordinates": [100.0, 0.0]},
    "linestring": {"type": "LineString", "coordinates": [[100.0, 0.0], [101.0, 1.0]]},
    "polygon": {
        "type": "Polygon",
        "coordinates": [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]],
    },
    "polygon_with_hole": {
        "type": "Polygon",
        "coordinates": [
            [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
            [[100.8, 0.8], [100.8, 0.2], [100.2, 0.2], [100.2, 0.8], [100.8, 0.8]],
        ],
    },
    "multilinestring": {
        "type": "MultiLineString",
        "coordinates": [[[100.0, 0.0], [101.0, 1.0]], [[102.0, 2.0], [103.0, 3.0]]],
    },
    "multipolygon_with_hole": {
        "type": "MultiPolygon",
        "coordinates": [
            [[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
            [
                [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
                [[100.2, 0.2], [100.2, 0.8], [100.8, 0.8], [100.8, 0.2], [100.2, 0.2]],
            ],
        ],
    },
    # A coordinate carrying more decimal digits than Float64 can hold: the round-trip is exact at
    # Float64 precision, so the oracle (which also parses into IEEE doubles) sees identical geometry.
    "high_precision": {
        "type": "Point",
        "coordinates": [100.123456789012345, 0.987654321098765],
    },
    # Antimeridian-crossing line (RFC 7946 section 3.1.9): coordinates must be preserved verbatim.
    "antimeridian": {"type": "LineString", "coordinates": [[170.0, 0.0], [-170.0, 0.0]]},
}


@pytest.mark.parametrize("name", sorted(ROUNDTRIP_GEOMETRIES))
def test_geometry_roundtrip_matches_oracle(start_cluster, name):
    original = ROUNDTRIP_GEOMETRIES[name]
    emitted = ch_roundtrip(original)

    # The geometry type must survive the round-trip.
    assert emitted["type"] == original["type"], f"{name}: type changed to {emitted['type']}"

    # The emitted geometry must be geometrically identical to the original, as judged by GEOS.
    original_geom = from_geojson(json.dumps(original))
    emitted_geom = from_geojson(json.dumps(emitted))
    assert equals_exact(
        original_geom, emitted_geom, tolerance=1e-9
    ), f"{name}: ClickHouse round-trip diverged from the oracle\n original: {original}\n emitted:  {emitted}"


def test_altitude_is_dropped(start_cluster):
    """A GeoJSON position may carry a third (altitude) element; ClickHouse's `Geometry` type is 2D, so
    the altitude is dropped. This documents that lossy behavior explicitly: the emitted position is 2D
    and its X/Y match, per the oracle."""
    original = {"type": "Point", "coordinates": [1.5, 2.5, 99.0]}
    emitted = ch_roundtrip(original)

    assert emitted == {"type": "Point", "coordinates": [1.5, 2.5]}
    # The oracle confirms the X/Y are preserved after the altitude is discarded.
    emitted_geom = from_geojson(json.dumps(emitted))
    assert list(get_coordinates(emitted_geom)[0]) == [1.5, 2.5]


def test_winding_order_is_preserved(start_cluster):
    """RFC 7946 (section 3.1.6) recommends right-hand-rule winding (exterior counter-clockwise, holes
    clockwise), but ClickHouse must neither require nor silently rewind rings. A polygon supplied with
    a clockwise exterior ring must come back with the exact same coordinate order, not rewound."""
    original = {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]],
    }
    emitted = ch_roundtrip(original)
    assert emitted["coordinates"] == original["coordinates"], "winding order was altered"


# Degenerate geometries that violate RFC 7946 shape rules. With validation enabled (the default) the
# input format rejects them; disabling it lets them through unchanged. This is the accept/reject
# asymmetry against a lenient oracle: Shapely's GEOS parser accepts several of these, so the interesting
# signal is that ClickHouse is stricter by default -- and, crucially, symmetric (its own reader accepts
# exactly what its writer can produce with validation disabled).
DEGENERATE_GEOMETRIES = {
    "linestring_one_point": {"type": "LineString", "coordinates": [[0.0, 0.0]]},
    "polygon_short_ring": {"type": "Polygon", "coordinates": [[[0.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]},
    "polygon_unclosed_ring": {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [2.0, 2.0]]],
    },
    "empty_multilinestring": {"type": "MultiLineString", "coordinates": []},
}


@pytest.mark.parametrize("name", sorted(DEGENERATE_GEOMETRIES))
def test_degenerate_geometry_rejected_by_default(start_cluster, name):
    geometry = DEGENERATE_GEOMETRIES[name]
    document = _feature_collection(geometry).replace("'", "''")

    # Rejected by default (RFC 7946 validity enforced).
    error = node.query_and_get_error(
        f"SELECT count() FROM format(GeoJSON, '{document}')"
    )
    assert "INCORRECT_DATA" in error, f"{name}: expected INCORRECT_DATA, got: {error}"

    # Accepted as-is when geometry validation is disabled.
    accepted = node.query(
        f"SELECT count() FROM format(GeoJSON, '{document}') "
        "SETTINGS format_geojson_validate_geometry = 0"
    )
    assert accepted.strip() == "1", f"{name}: not accepted with validation disabled"
