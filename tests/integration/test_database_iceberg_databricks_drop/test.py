import json
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

# Must match the constants in mock_servers/databricks_catalog.py.
TOKEN = "dapi-test-token"
CATALOG = "unity"
SCHEMA = "sch#\u00e9"

MOCK_PORT = 8780
REST_URL = f"http://localhost:{MOCK_PORT}/api/2.1/unity-catalog/iceberg-rest"
TABLES_PATH = f"/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/{CATALOG}/namespaces/{SCHEMA}/tables"
DATABASE = "databricks"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock_servers"),
            [("databricks_catalog.py", "node", MOCK_PORT)],
        )
        yield cluster
    finally:
        cluster.shutdown()


def mock_request(method, path, body=None):
    command = ["curl", "-sS", "--fail", "-X", method, f"http://localhost:{MOCK_PORT}{path}"]
    if body is not None:
        command += ["-H", "Content-Type: application/json", "--data-binary", json.dumps(body)]
    return node.exec_in_container(command)


def catalog_deletes():
    requests = json.loads(mock_request("GET", "/control/requests"))
    return [request for request in requests if request.startswith("DELETE ")]


def test_drop_follows_unity_table_type(started_cluster):
    mock_request("POST", "/control/reset", {"managed": "MANAGED", "external": "EXTERNAL", "foreign": "FOREIGN", "untyped": ""})
    node.query(
        f"""
        DROP DATABASE IF EXISTS {DATABASE};
        CREATE DATABASE {DATABASE} ENGINE = DataLakeCatalog('{REST_URL}')
        SETTINGS catalog_type = 'rest', warehouse = '{CATALOG}', auth_header = 'Authorization: Bearer {TOKEN}',
            vended_credentials = 0
        """,
        settings={"allow_database_iceberg": 1},
    )

    # Databricks deletes the data of a managed table whatever `purgeRequested` says.
    error = node.query_and_get_error(f"DROP TABLE {DATABASE}.`{SCHEMA}.managed`")
    assert "SUPPORT_IS_DISABLED" in error and "MANAGED" in error, error
    assert catalog_deletes() == []
    assert node.query(f"EXISTS TABLE {DATABASE}.`{SCHEMA}.managed`") == "1\n"

    node.query(f"DROP TABLE {DATABASE}.`{SCHEMA}.managed`", settings={"iceberg_delete_data_on_drop": 1})
    assert catalog_deletes() == [f"DELETE {TABLES_PATH}/managed?purgeRequested=true"]
    assert node.query(f"EXISTS TABLE {DATABASE}.`{SCHEMA}.managed`") == "0\n"

    # Databricks keeps the data of an external table.
    node.query(f"DROP TABLE {DATABASE}.`{SCHEMA}.external`")
    assert catalog_deletes() == [
        f"DELETE {TABLES_PATH}/managed?purgeRequested=true",
        f"DELETE {TABLES_PATH}/external?purgeRequested=false",
    ]

    # Nothing is known about what Databricks does with the data of other table types.
    error = node.query_and_get_error(f"DROP TABLE {DATABASE}.`{SCHEMA}.foreign`")
    assert "SUPPORT_IS_DISABLED" in error and "FOREIGN" in error, error
    assert len(catalog_deletes()) == 2
    assert node.query(f"EXISTS TABLE {DATABASE}.`{SCHEMA}.foreign`") == "1\n"

    # A table whose type the Unity API does not report is refused whatever the setting says.
    for delete_data in (0, 1):
        error = node.query_and_get_error(
            f"DROP TABLE {DATABASE}.`{SCHEMA}.untyped`", settings={"iceberg_delete_data_on_drop": delete_data}
        )
        assert "SUPPORT_IS_DISABLED" in error, error
    assert len(catalog_deletes()) == 2
    assert node.query(f"EXISTS TABLE {DATABASE}.`{SCHEMA}.untyped`") == "1\n"

    node.query(f"DROP DATABASE {DATABASE}")
