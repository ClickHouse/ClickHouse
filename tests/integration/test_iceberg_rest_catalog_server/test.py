#!/usr/bin/env python3

import base64
import time
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/iceberg_rest_catalog.xml"],
    stay_alive=True,
)

DEFAULT_AUTH = ("default", "")

CATALOG_PORT = 8182


def wait_catalog_ready(timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            requests.get(catalog_url("/v1/config"), timeout=1)
            return
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
    raise AssertionError("Catalog port did not start accepting connections")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        wait_catalog_ready()
        yield cluster
    finally:
        cluster.shutdown()


def catalog_url(path):
    return f"http://{node.ip_address}:{CATALOG_PORT}{path}"


def catalog_request(
    method, path, json=None, params=None, expected_code=200, auth=None, headers=None
):
    response = requests.request(
        method, catalog_url(path), json=json, params=params, auth=auth, headers=headers
    )
    assert response.status_code == expected_code, (
        f"Expected {expected_code}, got {response.status_code}. "
        f"Response: {response.text}"
    )
    return response


def assert_error_shape(response, expected_type):
    error = response.json()["error"]
    assert error["type"] == expected_type
    assert error["code"] == response.status_code
    assert error["message"]


def create_namespace(name_levels, properties=None, expected_code=200):
    body = {"namespace": name_levels}
    if properties is not None:
        body["properties"] = properties
    return catalog_request(
        "POST", "/v1/my_warehouse/namespaces", json=body, expected_code=expected_code
    )


def list_namespaces(parent=None):
    params = {"parent": parent} if parent is not None else None
    response = catalog_request("GET", "/v1/my_warehouse/namespaces", params=params)
    return response.json()["namespaces"]


def test_config(started_cluster):
    response = catalog_request(
        "GET", "/v1/config", params={"warehouse": "my_warehouse"}
    )
    result = response.json()
    assert result["overrides"]["prefix"] == "my_warehouse"
    assert result["defaults"] == {}
    assert result["endpoints"] == [
        "GET /v1/config",
        "GET /v1/{prefix}/namespaces",
        "POST /v1/{prefix}/namespaces",
        "HEAD /v1/{prefix}/namespaces/{namespace}",
    ]

    response = catalog_request("GET", "/v1/config", expected_code=400)
    assert_error_shape(response, "BadRequestException")

    response = catalog_request(
        "GET", "/v1/config", params={"warehouse": "unknown"}, expected_code=404
    )
    assert_error_shape(response, "NoSuchWarehouseException")


def test_namespaces(started_cluster):
    ns = f"sales_{uuid.uuid4().hex[:8]}"

    response = create_namespace([ns], properties={"location": "s3://bucket/sales/"})
    result = response.json()
    assert result["namespace"] == [ns]
    assert result["properties"] == {"location": "s3://bucket/sales/"}

    response = create_namespace([ns], expected_code=409)
    assert_error_shape(response, "AlreadyExistsException")

    create_namespace([ns, "eu"])

    top_level = list_namespaces()
    assert [ns] in top_level
    assert [ns, "eu"] not in top_level

    assert list_namespaces(parent=ns) == [[ns, "eu"]]

    # Multi-part namespaces are joined with the unit separator 0x1F (url encoded `%1F`),
    # which is the spec default when /config does not override `namespace-separator`.
    create_namespace([ns, "eu", "west"])
    assert list_namespaces(parent=ns) == [[ns, "eu"]]
    assert list_namespaces(parent=f"{ns}\x1feu") == [[ns, "eu", "west"]]
    assert list_namespaces(parent=f"{ns}\x1feu\x1fwest") == []

    response = requests.head(
        catalog_url(f"/v1/my_warehouse/namespaces/{ns}%1Feu%1Fwest")
    )
    assert response.status_code == 204, response.text

    response = catalog_request(
        "GET",
        "/v1/my_warehouse/namespaces",
        params={"parent": f"{ns}\x1fmissing"},
        expected_code=404,
    )
    assert_error_shape(response, "NoSuchNamespaceException")

    response = catalog_request(
        "GET",
        "/v1/my_warehouse/namespaces",
        params={"parent": "missing"},
        expected_code=404,
    )
    assert_error_shape(response, "NoSuchNamespaceException")


def test_create_namespace_creates_parents(started_cluster):
    ns = f"parents_{uuid.uuid4().hex[:8]}"

    create_namespace([ns, "eu", "west"])

    assert [ns] in list_namespaces()
    assert list_namespaces(parent=ns) == [[ns, "eu"]]
    assert list_namespaces(parent=f"{ns}\x1feu") == [[ns, "eu", "west"]]

    # The parents are created with empty properties, and creating them again conflicts.
    response = create_namespace([ns, "eu"], expected_code=409)
    assert_error_shape(response, "AlreadyExistsException")


def test_namespace_exists(started_cluster):
    ns = f"exists_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    response = requests.head(catalog_url(f"/v1/my_warehouse/namespaces/{ns}"))
    assert response.status_code == 204, response.text
    assert response.text == ""

    response = requests.head(catalog_url("/v1/my_warehouse/namespaces/missing"))
    assert response.status_code == 404, response.text
    assert response.text == ""


def test_malformed_create_namespace(started_cluster):
    for body in [None, {}, {"namespace": []}, {"namespace": [""]}]:
        response = requests.post(catalog_url("/v1/my_warehouse/namespaces"), json=body)
        assert response.status_code == 400, response.text
        assert_error_shape(response, "BadRequestException")


def test_not_implemented(started_cluster):
    response = catalog_request(
        "GET", "/v1/my_warehouse/namespaces/sales/tables", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = catalog_request(
        "POST", "/v1/my_warehouse/namespaces/sales/tables", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = catalog_request(
        "POST", "/v1/my_warehouse/tables/rename", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = requests.head(catalog_url("/v1/my_warehouse/namespaces/sales/tables/t"))
    assert response.status_code == 406

    # Views and functions are part of the spec, but there are no plans for this server to support them.
    response = catalog_request(
        "GET", "/v1/my_warehouse/namespaces/sales/functions", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = catalog_request(
        "GET", "/v1/my_warehouse/namespaces/sales/views", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")


def test_not_found_shape(started_cluster):
    response = catalog_request("GET", "/v1/nonsense", expected_code=404)
    assert_error_shape(response, "NotFoundException")

    response = catalog_request(
        "GET", "/v1/other_warehouse/namespaces", expected_code=404
    )
    assert_error_shape(response, "NotFoundException")


def test_stop_start_listen(started_cluster):
    ns = f"persistent_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    node.query("SYSTEM STOP LISTEN ICEBERG REST CATALOG")
    for _ in range(100):
        try:
            requests.get(catalog_url("/v1/config"), timeout=1)
        except requests.exceptions.ConnectionError:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("Catalog port is still accepting connections")

    node.query("SYSTEM START LISTEN ICEBERG REST CATALOG")
    for _ in range(100):
        try:
            requests.get(catalog_url("/v1/config"), timeout=1)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
    else:
        raise AssertionError("Catalog port did not start accepting connections")

    assert [ns] in list_namespaces()


def test_authentication(started_cluster):
    # (basic auth, headers, is_ok)
    cases = [
        (DEFAULT_AUTH, None, True),
        (("default", "wrong"), None, False),
        (("no_such_user", ""), None, False),
        (None, {"X-ClickHouse-User": "default", "X-ClickHouse-Key": ""}, True),
        (None, {"X-ClickHouse-User": "default", "X-ClickHouse-Key": "wrong"}, False),
    ]
    for auth, headers, is_ok in cases:
        response = catalog_request(
            "GET",
            "/v1/my_warehouse/namespaces",
            expected_code=200 if is_ok else 401,
            auth=auth,
            headers=headers,
        )
        if not is_ok:
            assert_error_shape(response, "NotAuthorizedException")


def test_auth_failure_does_not_poison_connection(started_cluster):
    session = requests.Session()
    response = session.get(
        catalog_url("/v1/my_warehouse/namespaces"), auth=("default", "wrong")
    )
    assert response.status_code == 401
    response = session.get(
        catalog_url("/v1/my_warehouse/namespaces"), auth=DEFAULT_AUTH
    )
    assert response.status_code == 200


def test_client_auth_header(started_cluster):
    # (credentials, is_ok)
    cases = [
        (b"default:", True),
        (b"default:wrong", False),
    ]
    for credentials, is_ok in cases:
        token = base64.b64encode(credentials).decode()
        # The client fetches /v1/config on CREATE DATABASE, so wrong credentials fail there with the server's 401.
        query = f"""
            DROP DATABASE IF EXISTS rest_client_auth_db;
            SET allow_experimental_database_iceberg = 1;
            CREATE DATABASE rest_client_auth_db
            ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1')
            SETTINGS catalog_type = 'rest', warehouse = 'my_warehouse',
                auth_header = 'Authorization: Basic {token}'
            """
        if is_ok:
            node.query(query)
            node.query("DROP DATABASE IF EXISTS rest_client_auth_db")
        else:
            error = node.query_and_get_error(query)
            assert "401" in error, error


def test_clickhouse_rest_catalog_client(started_cluster):
    ns = f"client_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    node.query(f"""
        DROP DATABASE IF EXISTS rest_client_db;
        SET allow_experimental_database_iceberg = 1;
        CREATE DATABASE rest_client_db
        ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1')
        SETTINGS catalog_type = 'rest', warehouse = 'my_warehouse'
        """)
    node.query("DROP DATABASE IF EXISTS rest_client_db")
