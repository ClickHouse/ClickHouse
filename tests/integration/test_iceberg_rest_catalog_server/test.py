#!/usr/bin/env python3

import base64
import time
import uuid

import pytest
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/iceberg_rest_catalog.xml",
        "configs/named_collections.xml",
    ],
    user_configs=["configs/users.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)

node_aux = cluster.add_instance(
    "node_aux",
    main_configs=[
        "configs/iceberg_rest_catalog_aux.xml",
        "configs/auxiliary_zookeeper.xml",
        "configs/named_collections.xml",
    ],
    with_minio=True,
    with_zookeeper=True,
)

DEFAULT_AUTH = ("default", "")

CATALOG_PORT = 8182
KEEPER_ROOT = "/clickhouse/iceberg_rest_catalog/my_warehouse"
FORMAT_MARKER = b"IcebergRESTCatalog\nformat_version: 1"
BUCKET = "warehouse"
BASE_LOCATION = f"s3://{BUCKET}/my_warehouse"

# Iceberg JSON schema as a client sends it in CreateTableRequest.
DEFAULT_SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [],
    "fields": [
        {"id": 1, "name": "id", "required": True, "type": "long"},
        {"id": 2, "name": "name", "required": False, "type": "string"},
    ],
}


def wait_catalog_ready(timeout=60, instance=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            requests.get(catalog_url("/v1/config", instance), timeout=1)
            return
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
    raise AssertionError("Catalog port did not start accepting connections")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        get_keeper().ensure_path("/aux_root")
        if not cluster.minio_client.bucket_exists(BUCKET):
            cluster.minio_client.make_bucket(BUCKET)
        wait_catalog_ready()
        wait_catalog_ready(instance=node_aux)
        yield cluster
    finally:
        cluster.shutdown()


def catalog_url(path, instance=None):
    instance = instance or node
    return f"http://{instance.ip_address}:{CATALOG_PORT}{path}"


def get_keeper():
    return cluster.get_kazoo_client("zoo1")


def restart_node():
    node.restart_clickhouse()
    wait_catalog_ready()


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


def tables_url(ns, table=None):
    url = f"/v1/my_warehouse/namespaces/{ns}/tables"
    if table is not None:
        url += f"/{table}"
    return url


def create_table(ns, name, schema=DEFAULT_SCHEMA, expected_code=200, auth=None, **extra):
    body = {"name": name, "schema": schema, **extra}
    return catalog_request(
        "POST", tables_url(ns), json=body, expected_code=expected_code, auth=auth
    )


def list_tables(ns):
    response = catalog_request("GET", tables_url(ns))
    return [identifier["name"] for identifier in response.json()["identifiers"]]


def table_exists(ns, table):
    response = requests.head(catalog_url(tables_url(ns, table)))
    assert response.status_code in (204, 404), response.text
    assert response.text == ""
    return response.status_code == 204


def drop_table(ns, table, expected_code=204, auth=None):
    return catalog_request(
        "DELETE", tables_url(ns, table), expected_code=expected_code, auth=auth
    )


def metadata_key(metadata_location):
    prefix = f"s3://{BUCKET}/"
    assert metadata_location.startswith(prefix), metadata_location
    return metadata_location[len(prefix) :]


def list_metadata_files(location):
    prefix = metadata_key(location) + "/metadata/"
    return [
        obj.object_name
        for obj in cluster.minio_client.list_objects(BUCKET, prefix, recursive=True)
    ]


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
        "GET /v1/{prefix}/namespaces/{namespace}",
        "HEAD /v1/{prefix}/namespaces/{namespace}",
        "GET /v1/{prefix}/namespaces/{namespace}/tables",
        "POST /v1/{prefix}/namespaces/{namespace}/tables",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    ]

    response = catalog_request("GET", "/v1/config", expected_code=400)
    assert_error_shape(response, "BadRequestException")

    response = catalog_request(
        "GET", "/v1/config", params={"warehouse": "unknown"}, expected_code=404
    )
    assert_error_shape(response, "NoSuchWarehouseException")


def test_namespaces(started_cluster):
    ns = f"sales_{uuid.uuid4().hex[:8]}"

    # The location must be inside the warehouse bucket.
    location = f"s3://{BUCKET}/sales/"
    response = create_namespace([ns], properties={"location": location})
    result = response.json()
    assert result["namespace"] == [ns]
    assert result["properties"] == {"location": location}

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


def test_load_namespace(started_cluster):
    ns = f"load_{uuid.uuid4().hex[:8]}"
    create_namespace([ns], properties={"owner": "asya"})

    response = catalog_request("GET", f"/v1/my_warehouse/namespaces/{ns}")
    assert response.json() == {"namespace": [ns], "properties": {"owner": "asya"}}

    catalog_request("GET", "/v1/my_warehouse/namespaces/missing", expected_code=404)


def test_malformed_create_namespace(started_cluster):
    for body in [None, {}, {"namespace": []}, {"namespace": [""]}]:
        response = requests.post(catalog_url("/v1/my_warehouse/namespaces"), json=body)
        assert response.status_code == 400, response.text
        assert_error_shape(response, "BadRequestException")


def test_not_implemented(started_cluster):
    response = catalog_request(
        "POST", "/v1/my_warehouse/tables/rename", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = catalog_request(
        "POST", "/v1/my_warehouse/namespaces/sales/tables/t", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = catalog_request(
        "POST", "/v1/my_warehouse/namespaces/sales/register", expected_code=406
    )
    assert_error_shape(response, "UnsupportedOperationException")

    response = requests.head(catalog_url("/v1/my_warehouse/namespaces/sales/views/v"))
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


def test_survives_server_restart(started_cluster):
    ns = f"restart_{uuid.uuid4().hex[:8]}"
    create_namespace([ns], properties={"owner": "asya"})
    create_namespace([ns, "eu"])

    # A full restart drops all server state, so the namespaces must come back from Keeper.
    restart_node()

    assert [ns] in list_namespaces()
    assert list_namespaces(parent=ns) == [[ns, "eu"]]

    response = requests.head(catalog_url(f"/v1/my_warehouse/namespaces/{ns}%1Feu"))
    assert response.status_code == 204, response.text

    zk = get_keeper()
    assert zk.get(f"{KEEPER_ROOT}/namespaces/{ns}")[0] == b'{"owner":"asya"}'


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


def test_create_namespace_rejects_readonly(started_cluster):
    ns = f"forbidden_{uuid.uuid4().hex[:8]}"
    body = {"namespace": [ns]}

    # Reading is allowed for everyone who can log in.
    catalog_request("GET", "/v1/my_warehouse/namespaces", auth=("readonly_user", ""))

    response = catalog_request(
        "POST",
        "/v1/my_warehouse/namespaces",
        json=body,
        auth=("readonly_user", ""),
        expected_code=403,
    )
    assert_error_shape(response, "ForbiddenException")
    assert "readonly" in response.json()["error"]["message"]
    assert [ns] not in list_namespaces()

    # `allow_ddl = 0` permits writes but not structural changes, and a namespace is structure.
    catalog_request("GET", "/v1/my_warehouse/namespaces", auth=("no_ddl_user", ""))
    response = catalog_request(
        "POST",
        "/v1/my_warehouse/namespaces",
        json=body,
        auth=("no_ddl_user", ""),
        expected_code=403,
    )
    assert_error_shape(response, "ForbiddenException")
    assert "DDL" in response.json()["error"]["message"]
    assert [ns] not in list_namespaces()

    create_namespace([ns])
    assert [ns] in list_namespaces()


def test_table_ddl_rejects_readonly(started_cluster):
    ns = f"forbidden_tables_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])
    create_table(ns, "existing")

    # `allow_ddl = 0` permits writes but not structural changes, and a table is structure.
    for user in ["readonly_user", "no_ddl_user"]:
        auth = (user, "")
        # Reads are allowed.
        catalog_request("GET", tables_url(ns, "existing"), auth=auth)

        response = create_table(ns, "forbidden", auth=auth, expected_code=403)
        assert_error_shape(response, "ForbiddenException")
        response = drop_table(ns, "existing", auth=auth, expected_code=403)
        assert_error_shape(response, "ForbiddenException")

    assert list_tables(ns) == ["existing"]


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
    create_table(ns, "events")

    node.query(f"""
        DROP DATABASE IF EXISTS rest_client_db;
        SET allow_experimental_database_iceberg = 1;
        CREATE DATABASE rest_client_db
        ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1', '{minio_access_key}', '{minio_secret_key}')
        SETTINGS catalog_type = 'rest', warehouse = 'my_warehouse',
            storage_endpoint = 'http://minio1:9001/{BUCKET}'
        """)
    try:
        tables = node.query("SHOW TABLES FROM rest_client_db").split()
        assert f"{ns}.events" in tables
        assert node.query(f"SELECT count() FROM rest_client_db.`{ns}.events`") == "0\n"
        assert node.query(
            f"DESCRIBE rest_client_db.`{ns}.events` FORMAT TSVRaw SETTINGS describe_compact_output = 1"
        ) == "id\tInt64\nname\tNullable(String)\n"
    finally:
        node.query("DROP DATABASE IF EXISTS rest_client_db")


def test_clickhouse_rest_catalog_client_create_table(started_cluster):
    ns = f"client_create_{uuid.uuid4().hex[:8]}"

    node.query(f"""
        DROP DATABASE IF EXISTS rest_client_create_db;
        SET allow_experimental_database_iceberg = 1;
        CREATE DATABASE rest_client_create_db
        ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1', '{minio_access_key}', '{minio_secret_key}')
        SETTINGS catalog_type = 'rest', warehouse = 'my_warehouse',
            storage_endpoint = 'http://minio1:9001/{BUCKET}'
        """)

    # The client sends the table location to the server. The server accepts `s3://` locations only.
    node.query(
        f"""
        CREATE TABLE rest_client_create_db.`{ns}.events` (id Int64, name String)
        ENGINE = IcebergS3('http://minio1:9001/{BUCKET}/{ns}/events/', '{minio_access_key}', '{minio_secret_key}')
        """,
        settings={"write_full_path_in_iceberg_metadata": 1},
    )

    # INSERT is not tested here: it commits through UpdateTable, which the server does not support yet.
    assert node.query(f"SELECT count() FROM rest_client_create_db.`{ns}.events`") == "0\n"
    assert list_tables(ns) == ["events"]

    node.query("DROP DATABASE IF EXISTS rest_client_create_db")


def test_keeper_layout(started_cluster):
    ns = f"layout-{uuid.uuid4().hex[:8]}"
    create_namespace([ns, "eu-west"], properties={"owner": "asya"})
    result = create_table(f"{ns}\x1feu-west", "my.table").json()

    zk = get_keeper()
    assert zk.get(KEEPER_ROOT)[0] == FORMAT_MARKER

    # Levels are escaped like file names, so the tree stays walkable with a Keeper client.
    escaped = ns.replace("-", "%2D")
    parent_path = f"{KEEPER_ROOT}/namespaces/{escaped}"
    child_path = f"{parent_path}/namespaces/eu%2Dwest"
    assert sorted(zk.get_children(parent_path)) == ["namespaces", "tables"]
    assert sorted(zk.get_children(child_path)) == ["namespaces", "tables"]
    assert zk.get(parent_path)[0] == b"{}"
    assert zk.get(child_path)[0] == b'{"owner":"asya"}'

    # The uuid is in the path, so a commit cannot target a table re-created under the same name.
    table_path = f"{child_path}/tables/my%2Etable"
    table_uuid = result["metadata"]["table-uuid"]
    assert zk.get_children(f"{child_path}/tables") == ["my%2Etable"]
    assert zk.get(table_path)[0] == table_uuid.encode()
    assert zk.get_children(table_path) == [table_uuid]
    assert zk.get(f"{table_path}/{table_uuid}")[0] == result["metadata-location"].encode()

    assert list_namespaces(parent=ns) == [[ns, "eu-west"]]
    assert list_tables(f"{ns}\x1feu-west") == ["my.table"]


def test_create_and_load_table(started_cluster):
    ns = f"tables_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    result = create_table(ns, "events").json()
    metadata = result["metadata"]
    table_uuid = metadata["table-uuid"]
    assert metadata["format-version"] == 2
    assert metadata["location"] == f"{BASE_LOCATION}/{ns}/events-{table_uuid}"
    assert metadata["schemas"][0]["fields"] == DEFAULT_SCHEMA["fields"]
    assert metadata["last-column-id"] == 2
    assert metadata["partition-specs"] == [{"spec-id": 0, "fields": []}]
    assert metadata["snapshots"] == []
    assert result["metadata-location"] == (
        f"{metadata['location']}/metadata/v1-{table_uuid}.metadata.json"
    )

    loaded = catalog_request("GET", tables_url(ns, "events")).json()
    assert loaded == result

    # Creating it again leaks no second metadata file next to the first one.
    response = create_table(ns, "events", location=metadata["location"], expected_code=409)
    assert_error_shape(response, "TableAlreadyExistsException")
    assert list_metadata_files(metadata["location"]) == [
        metadata_key(result["metadata-location"])
    ]

    # Nested ids count, partition fields reference the schema, and `format-version` is not a property.
    schema_nested = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "a", "required": False, "type": "int"},
            {
                "id": 2,
                "name": "m",
                "required": False,
                "type": {"type": "map", "key-id": 3, "key": "string", "value-id": 4, "value": "int", "value-required": False},
            },
        ],
    }
    spec = {"spec-id": 5, "fields": [{"source-id": 1, "field-id": 1001, "name": "a_p", "transform": "identity"}]}
    order = {"order-id": 7, "fields": [{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}
    result = create_table(
        ns,
        "nested",
        schema=schema_nested,
        **{"partition-spec": spec, "write-order": order, "properties": {"format-version": "2", "owner": "asya"}},
    ).json()
    assert result["metadata"]["last-column-id"] == 4
    assert result["metadata"]["last-partition-id"] == 1001
    assert result["metadata"]["partition-specs"][0]["spec-id"] == 0
    # Order id 0 is reserved for the unsorted order, so a sorted table gets 1.
    assert result["metadata"]["sort-orders"][0]["order-id"] == 1
    assert result["metadata"]["default-sort-order-id"] == 1
    assert result["metadata"]["properties"] == {"owner": "asya"}


def test_table_location(started_cluster):
    ns = f"location_{uuid.uuid4().hex[:8]}"
    ns_location = f"s3://{BUCKET}/custom/{ns}"
    create_namespace([ns], properties={"location": ns_location + "/"})

    result = create_table(ns, "events").json()
    table_uuid = result["metadata"]["table-uuid"]
    assert result["metadata"]["location"] == f"{ns_location}/events-{table_uuid}"
    cluster.minio_client.stat_object(BUCKET, metadata_key(result["metadata-location"]))

    # An explicit table location wins over the namespace location.
    location = f"s3://{BUCKET}/explicit/{ns}"
    result = create_table(ns, "explicit", location=location + "/").json()
    assert result["metadata"]["location"] == location
    assert result["metadata-location"].startswith(location + "/metadata/v1-")
    cluster.minio_client.stat_object(BUCKET, metadata_key(result["metadata-location"]))

    # The server has credentials for one bucket only, so a location elsewhere is refused up front.
    response = create_table(ns, "elsewhere", location="s3://other-bucket/path", expected_code=400)
    assert_error_shape(response, "BadRequestException")
    assert not table_exists(ns, "elsewhere")


def test_list_and_exists_and_drop(started_cluster):
    ns = f"listing_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])
    assert list_tables(ns) == []

    create_table(ns, "b_table")
    result = create_table(ns, "a_table").json()
    response = catalog_request("GET", tables_url(ns))
    assert response.json()["identifiers"] == [
        {"namespace": [ns], "name": "a_table"},
        {"namespace": [ns], "name": "b_table"},
    ]
    assert table_exists(ns, "a_table")
    assert not table_exists(ns, "c_table")

    # Purge must delete the data files too, which is not implemented, so the flag is refused rather than ignored.
    response = catalog_request(
        "DELETE", tables_url(ns, "a_table"), params={"purgeRequested": "true"}, expected_code=400
    )
    assert_error_shape(response, "BadRequestException")
    assert table_exists(ns, "a_table")

    drop_table(ns, "a_table")
    assert not table_exists(ns, "a_table")
    assert list_tables(ns) == ["b_table"]
    # Files stay on object storage.
    cluster.minio_client.stat_object(BUCKET, metadata_key(result["metadata-location"]))

    response = catalog_request("GET", tables_url(ns, "a_table"), expected_code=404)
    assert_error_shape(response, "NoSuchTableException")
    response = catalog_request("GET", tables_url("missing"), expected_code=404)
    assert_error_shape(response, "NoSuchNamespaceException")


def test_malformed_create_table(started_cluster):
    ns = f"malformed_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    duplicate_ids = {"type": "struct", "fields": [{"id": 1, "name": "a", "type": "int"}, {"id": 1, "name": "b", "type": "int"}]}
    bad_spec = {"fields": [{"source-id": 9, "field-id": 1000, "name": "p", "transform": "identity"}]}
    bodies = [
        {"schema": DEFAULT_SCHEMA},  # no name
        {"name": "", "schema": DEFAULT_SCHEMA},
        {"name": "t"},  # no schema
        {"name": "t", "schema": duplicate_ids},
        {"name": "t", "schema": DEFAULT_SCHEMA, "stage-create": True},
        {"name": "t", "schema": DEFAULT_SCHEMA, "partition-spec": bad_spec},
        {"name": "t", "schema": DEFAULT_SCHEMA, "properties": {"format-version": "3"}},
    ]
    for body in bodies:
        response = catalog_request("POST", tables_url(ns), json=body, expected_code=400)
        assert_error_shape(response, "BadRequestException")
    assert list_tables(ns) == []

    response = create_table("missing", "t", expected_code=404)
    assert_error_shape(response, "NoSuchNamespaceException")


def test_pyiceberg_client(started_cluster):
    ns = f"pyiceberg_{uuid.uuid4().hex[:8]}"
    catalog = load_catalog(
        "ch",
        **{
            "uri": catalog_url(""),
            "type": "rest",
            "warehouse": "my_warehouse",
            "auth": {"type": "basic", "basic": {"username": "default", "password": ""}},
            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )

    catalog.create_namespace(ns)
    assert (ns,) in catalog.list_namespaces()

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    table = catalog.create_table(f"{ns}.events", schema=schema)
    assert table.metadata_location.startswith(f"{BASE_LOCATION}/{ns}/events-")
    assert table.schema().as_struct() == schema.as_struct()
    assert table.current_snapshot() is None

    loaded = catalog.load_table(f"{ns}.events")
    assert loaded.metadata_location == table.metadata_location
    assert loaded.schema().as_struct() == schema.as_struct()

    assert catalog.list_tables(ns) == [(ns, "events")]
    assert catalog.table_exists(f"{ns}.events")

    catalog.drop_table(f"{ns}.events")
    assert catalog.list_tables(ns) == []
    assert not catalog.table_exists(f"{ns}.events")


def test_unsupported_format_is_refused(started_cluster):
    zk = get_keeper()
    zk.set(KEEPER_ROOT, b"IcebergRESTCatalog\nformat_version: 999")
    try:
        # The marker is checked when a new Keeper session is opened.
        restart_node()
        for _ in range(2):
            response = catalog_request(
                "GET", "/v1/my_warehouse/namespaces", expected_code=500
            )
            assert_error_shape(response, "InternalServerError")
        assert node.contains_in_log("has an unsupported format")
    finally:
        zk.set(KEEPER_ROOT, FORMAT_MARKER)
        restart_node()

    list_namespaces()


def test_auxiliary_keeper(started_cluster):
    # node_aux stores its state in the aux Keeper, which is chrooted to /aux_root.
    url = catalog_url("/v1/my_warehouse/namespaces", node_aux)
    assert requests.post(url, json={"namespace": ["aux_ns"]}).status_code == 200
    assert requests.get(url).json()["namespaces"] == [["aux_ns"]]

    zk = get_keeper()
    assert zk.exists(f"/aux_root{KEEPER_ROOT}/namespaces/aux_ns")
    assert not zk.exists(f"{KEEPER_ROOT}/namespaces/aux_ns")
