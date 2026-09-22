"""Creation and metadata time filtering with external tags at version 6."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

SERIES = {
    name: {"__name__": "http_requests", "job": "api", "instance": name, f"{name}_only": "yes"}
    for name in ("early", "current", "late")
}


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        # `Atomic` assigns inner UUIDs after normalization. It must not recreate
        # the `TAGS MIN MAX` target that normalization omitted for external tags.
        node.query("CREATE DATABASE external_tags ENGINE = Atomic")
        node.query(
            "CREATE TABLE external_tags.ext_tags ("
            "id UInt64, metric_name String, tags Map(String, String), "
            "min_time Nullable(DateTime64(3)), max_time Nullable(DateTime64(3))) "
            "ENGINE = Memory"
        )
        node.query(
            "CREATE TABLE external_tags.prometheus ENGINE = TimeSeries "
            "SETTINGS version = 6, store_min_time_and_max_time = 1 "
            "TAGS external_tags.ext_tags"
        )
        node.query(
            "INSERT INTO external_tags.prometheus (metric_name, tags, samples) VALUES "
            "('http_requests', {'job': 'api', 'instance': 'early', 'early_only': 'yes'}, [(1000, 1.0)]), "
            "('http_requests', {'job': 'api', 'instance': 'current', 'current_only': 'yes'}, [(2000, 2.0)]), "
            "('http_requests', {'job': 'api', 'instance': 'late', 'late_only': 'yes'}, [(3000, 3.0)])"
        )
        cluster.wait_for_url(f"http://{node.ip_address}:9093/external_tags/api/v1/labels")
        yield
    finally:
        cluster.shutdown()


def test_create_with_external_tags_in_atomic_database():
    assert node.query(
        "SELECT uuid != toUUID('00000000-0000-0000-0000-000000000000') "
        "FROM system.tables WHERE database = 'external_tags' AND name = 'prometheus'"
    ) == "1\n"
    assert "TAGS MIN MAX" not in node.query("SHOW CREATE TABLE external_tags.prometheus")
    assert node.query(
        "SELECT count() FROM system.tables "
        "WHERE database = 'external_tags' AND startsWith(name, '.inner_id.tagsminmax.')"
    ) == "0\n"
    assert node.query("SELECT count() FROM external_tags.ext_tags") == "3\n"


@pytest.mark.parametrize("selector", [None, '{job="api"}', 'http_requests{job="api"}'])
@pytest.mark.parametrize(
    "bounds,matching_series",
    [
        ({"start": 1900, "end": 2100}, ["current"]),
        ({"start": 2000, "end": 2000}, ["current"]),
        ({"start": 2100, "end": 2900}, []),
        ({"start": 2000}, ["current", "late"]),
        ({"end": 2000}, ["early", "current"]),
    ],
)
def test_metadata_time_bounds(bounds, matching_series, selector):
    series = [SERIES[name] for name in matching_series]
    labels = sorted({label for item in series for label in item})
    for endpoint, expected in (
        ("series", series),
        ("labels", labels),
        ("label/instance/values", sorted(matching_series)),
    ):
        params = dict(bounds)
        if selector is not None:
            params["match[]"] = selector
        elif endpoint == "series":
            # `/series` requires a selector; the label endpoints default to this matcher.
            params["match[]"] = '{__name__!=""}'
        response = requests.get(
            f"http://{node.ip_address}:9093/external_tags/api/v1/{endpoint}", params=params
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["status"] == "success", result
        actual = result["data"]
        if endpoint == "series":
            actual = sorted(sorted(item.items()) for item in actual)
            expected = sorted(sorted(item.items()) for item in expected)
        assert actual == expected, result
