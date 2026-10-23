#!/usr/bin/env python3

import base64
import gzip
import json
import time
import uuid
from urllib.parse import quote

import pytest
import requests
import urllib3
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from test_storage_s3.test_sts import run_s3_mocks

cluster = ClickHouseCluster(__file__)

# Ambient credentials of the server. Only the STS call is signed with them.
AWS_ENV = {"AWS_ACCESS_KEY_ID": "aws", "AWS_SECRET_ACCESS_KEY": "aws123"}

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/iceberg_rest_catalog.xml",
        "configs/s3.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    user_configs=["configs/users.xml"],
    env_variables=AWS_ENV,
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)

node_aux = cluster.add_instance(
    "node_aux",
    main_configs=[
        "configs/iceberg_rest_catalog_aux.xml",
        "configs/auxiliary_zookeeper.xml",
        "configs/s3.xml",
    ],
    env_variables=AWS_ENV,
    with_minio=True,
    with_zookeeper=True,
)

# Mock STS. It hands out the MinIO credentials when the session name and external id match.
sts = cluster.add_instance(
    name="sts.amazonaws.com",
    hostname="sts.amazonaws.com",
    image="clickhouse/python-bottle",
    tag="latest",
    stay_alive=True,
)
sts.stop_clickhouse(kill=True)

DEFAULT_AUTH = ("default", "")

CATALOG_PORT = 8182
CATALOG_SECURE_PORT = 8183
KEEPER_PARENT = "/clickhouse/iceberg_rest_catalog"
KEEPER_ROOT = f"{KEEPER_PARENT}/my_warehouse"
FORMAT = "IcebergRESTCatalog\nformat_version: 2"
BUCKET = "warehouse"
BASE_LOCATION = f"s3://{BUCKET}/my_warehouse"
ROLE_SESSION_NAME = "miniorole"
EXTERNAL_ID = "miniexternalid"
CREDENTIAL = {
    "type": "aws_role",
    "role_arn": "arn:aws:iam::123456789012:role/warehouse",
    "external_id": EXTERNAL_ID,
    "role_session_name": ROLE_SESSION_NAME,
}

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


def warehouse_definition(base_location, credential=CREDENTIAL):
    return {"format": FORMAT, "base_location": base_location, "credential": credential}


# TODO: Replace with `POST /management/v1/warehouses` when the management API lands.
def create_warehouse_in_keeper(name, base_location, keeper_parent=KEEPER_PARENT, credential=CREDENTIAL):
    path = f"{keeper_parent}/{name}"
    zk = get_keeper()
    zk.create(path, json.dumps(warehouse_definition(base_location, credential)).encode(), makepath=True)
    zk.create(f"{path}/namespaces")
    return path


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_s3_mocks(cluster, [ROLE_SESSION_NAME, EXTERNAL_ID])
        get_keeper().ensure_path("/aux_root")
        if not cluster.minio_client.bucket_exists(BUCKET):
            cluster.minio_client.make_bucket(BUCKET)
        create_warehouse_in_keeper("my_warehouse", BASE_LOCATION)
        # The aux Keeper is chrooted to /aux_root. A bucket root with a trailing slash as the base location.
        create_warehouse_in_keeper("my_warehouse", f"s3://{BUCKET}/", keeper_parent=f"/aux_root{KEEPER_PARENT}")
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


def test_namespace_location_with_embedded_bucket(started_cluster):
    # `s3://warehouse/s3://other/x` names bucket `warehouse` on the surface, but the S3 layer
    # re-parses the key and would write to `other`. Refuse it like a plain other-bucket location.
    ns = f"esc_{uuid.uuid4().hex[:8]}"
    for location in (
        f"s3://other/{ns}",
        f"s3://{BUCKET}/s3://other/{ns}",
        f"s3://{BUCKET}/{ns}/s3://other/x",
    ):
        create_namespace([ns], properties={"location": location}, expected_code=400)


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
    for body in [
        None,
        {},
        {"namespace": []},
        {"namespace": [""]},
        {"namespace": ["a\x1fb"]},
        {"namespace": ["ok", "a\x1fb"]},
    ]:
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


def wait_listening(url, expected):
    for _ in range(100):
        try:
            requests.get(url, timeout=1, verify=False)
            listening = True
        except requests.exceptions.ConnectionError:
            listening = False
        if listening == expected:
            return
        time.sleep(0.1)
    raise AssertionError(f"{url} listening={listening}, expected {expected}")


def test_stop_start_listen(started_cluster):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    ns = f"persistent_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    plain_url = catalog_url("/v1/config")
    secure_url = f"https://{node.ip_address}:{CATALOG_SECURE_PORT}/v1/config"

    # Both listeners share one server type, so one command stops both.
    node.query("SYSTEM STOP LISTEN ICEBERG REST CATALOG")
    wait_listening(plain_url, False)
    wait_listening(secure_url, False)

    node.query("SYSTEM START LISTEN ICEBERG REST CATALOG")
    wait_listening(plain_url, True)
    wait_listening(secure_url, True)

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
    level = "a b?#%41+/é&:;=@"
    create_namespace([ns, level], properties={"location": f"s3://{BUCKET}/{ns}_special"})
    create_table(f"{ns}%1F{quote(level, safe='')}", "events")
    create_table(ns, "t?#x")

    node.query(f"""
        DROP DATABASE IF EXISTS rest_client_db;
        SET allow_experimental_database_iceberg = 1;
        CREATE DATABASE rest_client_db
        ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1', '{minio_access_key}', '{minio_secret_key}')
        SETTINGS catalog_type = 'rest', warehouse = 'my_warehouse',
            storage_endpoint = 'http://minio1:9001/{BUCKET}'
        """)
    try:
        tables = node.query("SHOW TABLES FROM rest_client_db").splitlines()
        assert f"{ns}.events" in tables
        assert node.query(f"SELECT count() FROM rest_client_db.`{ns}.events`") == "0\n"
        assert node.query(
            f"DESCRIBE rest_client_db.`{ns}.events` FORMAT TSVRaw SETTINGS describe_compact_output = 1"
        ) == "id\tInt64\nname\tNullable(String)\n"
        assert f"{ns}.{level}.events" in tables
        assert node.contains_in_log(f"Requesting: .*/namespaces/{ns}%1F{quote(level, safe='')}/tables")
        assert f"{ns}.t%3F%23x" in tables
        assert node.query(f"SELECT count() FROM rest_client_db.`{ns}.{level}.events`") == "0\n"
        assert node.query(f"EXISTS TABLE rest_client_db.`{ns}.t%3F%23x`") == "1\n"
    finally:
        node.query("DROP DATABASE IF EXISTS rest_client_db")


def test_keeper_layout(started_cluster):
    ns = f"layout-{uuid.uuid4().hex[:8]}"
    create_namespace([ns, "eu-west"], properties={"owner": "asya"})
    result = create_table(f"{ns}\x1feu-west", "my.table").json()

    zk = get_keeper()
    assert json.loads(zk.get(KEEPER_ROOT)[0]) == warehouse_definition(BASE_LOCATION)

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
    response = create_table(ns, "events", expected_code=409)
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
    # Client spec-id and field-id are ignored. The server assigns its own.
    assert result["metadata"]["last-partition-id"] == 1000
    assert result["metadata"]["partition-specs"][0]["spec-id"] == 0
    assert result["metadata"]["partition-specs"][0]["fields"][0]["field-id"] == 1000
    # Order id 0 is reserved for the unsorted order, so a sorted table gets 1.
    assert result["metadata"]["sort-orders"][0]["order-id"] == 1
    assert result["metadata"]["default-sort-order-id"] == 1
    assert result["metadata"]["properties"] == {"owner": "asya"}


def test_partition_spec_field_ids(started_cluster):
    ns = f"spec_ids_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    # field-id is optional in CreateTableRequest. The server assigns 1000 + index.
    spec = {
        "fields": [
            {"source-id": 1, "name": "id_p", "transform": "identity"},
            {"source-id": 2, "name": "name_p", "transform": "identity"},
        ]
    }
    metadata = create_table(ns, "no_ids", **{"partition-spec": spec}).json()["metadata"]
    fields = metadata["partition-specs"][0]["fields"]
    assert [field["field-id"] for field in fields] == [1000, 1001]
    assert metadata["last-partition-id"] == 1001

    # Explicit field-ids are ignored, like the Java reference catalog does on create.
    # This also makes duplicate ids impossible.
    spec = {
        "fields": [
            {"source-id": 1, "field-id": 1005, "name": "id_p", "transform": "identity"},
            {"source-id": 2, "field-id": 1005, "name": "name_p", "transform": "identity"},
        ]
    }
    metadata = create_table(ns, "client_ids", **{"partition-spec": spec}).json()["metadata"]
    fields = metadata["partition-specs"][0]["fields"]
    assert [field["field-id"] for field in fields] == [1000, 1001]
    assert metadata["last-partition-id"] == 1001


def test_table_location(started_cluster):
    ns = f"location_{uuid.uuid4().hex[:8]}"
    ns_location = f"s3://{BUCKET}/custom/{ns}"
    create_namespace([ns], properties={"location": ns_location + "/"})

    result = create_table(ns, "events").json()
    table_uuid = result["metadata"]["table-uuid"]
    assert result["metadata"]["location"] == f"{ns_location}/events-{table_uuid}"
    cluster.minio_client.stat_object(BUCKET, metadata_key(result["metadata-location"]))

    # The catalog chooses every table path, so a client-chosen location is refused.
    response = create_table(ns, "explicit", location=f"s3://{BUCKET}/explicit/{ns}", expected_code=400)
    assert_error_shape(response, "BadRequestException")
    assert not table_exists(ns, "explicit")


def test_metadata_compression(started_cluster):
    ns = f"compression_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])

    # The codec comes from the standard table property. The spec spells gzip as `gz` in the file name.
    result = create_table(ns, "zipped", properties={"write.metadata.compression-codec": "gzip"}).json()
    table_uuid = result["metadata"]["table-uuid"]
    assert result["metadata-location"] == (
        f"{result['metadata']['location']}/metadata/v1-{table_uuid}.gz.metadata.json"
    )
    assert result["metadata"]["properties"] == {"write.metadata.compression-codec": "gzip"}
    raw = cluster.minio_client.get_object(BUCKET, metadata_key(result["metadata-location"])).read()
    assert json.loads(gzip.decompress(raw))["table-uuid"] == table_uuid
    # Loading reads the file back, so the server must decompress it.
    assert catalog_request("GET", tables_url(ns, "zipped")).json() == result

    # An explicit `none` is the default: plain file, plain name.
    result = create_table(ns, "plain", properties={"write.metadata.compression-codec": "none"}).json()
    assert result["metadata-location"].endswith(f"v1-{result['metadata']['table-uuid']}.metadata.json")
    raw = cluster.minio_client.get_object(BUCKET, metadata_key(result["metadata-location"])).read()
    assert json.loads(raw)["table-uuid"] == result["metadata"]["table-uuid"]

    # Only the two codecs of the spec. A `zstd` file could not be opened by other engines.
    response = create_table(ns, "bogus", properties={"write.metadata.compression-codec": "zstd"}, expected_code=400)
    assert_error_shape(response, "BadRequestException")
    assert not table_exists(ns, "bogus")


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
    unknown_type = {"type": "struct", "fields": [{"id": 1, "name": "x", "required": True, "type": "bogus"}]}
    numeric_type = {"type": "struct", "fields": [{"id": 1, "name": "x", "required": True, "type": 123}]}
    bad_element_type = {"type": "struct", "fields": [{"id": 1, "name": "x", "required": True, "type": {"type": "list", "element-id": 2, "element-required": True, "element": "bogus"}}]}
    bad_transform_spec = {"fields": [{"source-id": 1, "name": "p", "transform": "bogus"}]}
    bad_transform_order = {"fields": [{"source-id": 1, "transform": "bucket[x]", "direction": "asc", "null-order": "nulls-first"}]}
    bad_direction_order = {"fields": [{"source-id": 1, "transform": "identity", "direction": "up", "null-order": "nulls-first"}]}
    bad_null_order = {"fields": [{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "first"}]}
    bodies = [
        {"schema": DEFAULT_SCHEMA},  # no name
        {"name": "", "schema": DEFAULT_SCHEMA},
        {"name": "t"},  # no schema
        {"name": "t", "schema": duplicate_ids},
        {"name": "t", "schema": unknown_type},
        {"name": "t", "schema": numeric_type},
        {"name": "t", "schema": bad_element_type},
        {"name": "t", "schema": DEFAULT_SCHEMA, "stage-create": True},
        {"name": "t", "schema": DEFAULT_SCHEMA, "location": f"s3://{BUCKET}/explicit/t"},
        {"name": "t", "schema": DEFAULT_SCHEMA, "partition-spec": bad_spec},
        {"name": "t", "schema": DEFAULT_SCHEMA, "partition-spec": {"fields": [{"source-id": 1}]}},
        {"name": "t", "schema": DEFAULT_SCHEMA, "write-order": {"fields": [{"source-id": 1}]}},
        {"name": "t", "schema": DEFAULT_SCHEMA, "partition-spec": bad_transform_spec},
        {"name": "t", "schema": DEFAULT_SCHEMA, "write-order": bad_transform_order},
        {"name": "t", "schema": DEFAULT_SCHEMA, "write-order": bad_direction_order},
        {"name": "t", "schema": DEFAULT_SCHEMA, "write-order": bad_null_order},
        {"name": "t", "schema": DEFAULT_SCHEMA, "properties": {"format-version": "3"}},
    ]
    for body in bodies:
        response = catalog_request("POST", tables_url(ns), json=body, expected_code=400)
        assert_error_shape(response, "BadRequestException")
    assert list_tables(ns) == []

    response = create_table("missing", "t", expected_code=404)
    assert_error_shape(response, "NoSuchNamespaceException")


def pyiceberg_catalog():
    return load_catalog(
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


def test_pyiceberg_client(started_cluster):
    ns = f"pyiceberg_{uuid.uuid4().hex[:8]}"
    catalog = pyiceberg_catalog()

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


def test_names_with_slash(started_cluster):
    # pyiceberg sends `/` inside a name as `%2F`.
    catalog = pyiceberg_catalog()
    ns = f"slash_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace((ns, "x/y"))
    schema = Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True))

    for identifier in [(ns, "a/b"), (ns, "x/y", "a/b")]:
        catalog.create_table(identifier, schema=schema)
        assert catalog.table_exists(identifier)
        assert catalog.load_table(identifier).schema().as_struct() == schema.as_struct()
        assert catalog.list_tables(identifier[:-1]) == [identifier]
        catalog.drop_table(identifier)
        assert catalog.list_tables(identifier[:-1]) == []

    assert catalog.namespace_exists((ns, "x/y"))
    assert catalog.load_namespace_properties((ns, "x/y")) == {}


def test_slash_at_name_edge_does_not_retarget(started_cluster):
    # An encoded `/` at the edge of a name must not address the table without it.
    ns = f"edge_{uuid.uuid4().hex[:8]}"
    create_namespace([ns])
    create_table(ns, "a")
    for name in ["a%2F", "%2Fa"]:
        response = drop_table(ns, name, expected_code=404)
        assert_error_shape(response, "NoSuchTableException")
    assert table_exists(ns, "a")


def test_unsupported_format_is_refused(started_cluster):
    zk = get_keeper()
    definition = warehouse_definition(BASE_LOCATION)
    zk.set(KEEPER_ROOT, json.dumps({**definition, "format": "IcebergRESTCatalog\nformat_version: 999"}).encode())
    try:
        # The definition is re-read when the node changes, no restart needed.
        for _ in range(2):
            response = catalog_request(
                "GET", "/v1/my_warehouse/namespaces", expected_code=500
            )
            assert_error_shape(response, "InternalServerError")
        assert node.contains_in_log("has an unsupported format")
    finally:
        zk.set(KEEPER_ROOT, json.dumps(definition).encode())

    list_namespaces()


def test_warehouse_discovery(started_cluster):
    name = f"fresh_{uuid.uuid4().hex[:8]}"
    catalog_request("GET", "/v1/config", params={"warehouse": name}, expected_code=404)

    # A warehouse written to Keeper is served on the next request, on every server.
    path = create_warehouse_in_keeper(name, f"s3://{BUCKET}/{name}")
    response = catalog_request("GET", "/v1/config", params={"warehouse": name})
    assert response.json()["overrides"]["prefix"] == name
    namespaces_url = f"/v1/{name}/namespaces"
    catalog_request("POST", namespaces_url, json={"namespace": ["ns"]})
    result = catalog_request("POST", f"{namespaces_url}/ns/tables", json={"name": "t", "schema": DEFAULT_SCHEMA}).json()
    assert result["metadata"]["location"].startswith(f"s3://{BUCKET}/{name}/ns/t-")

    get_keeper().delete(path, recursive=True)
    catalog_request("GET", "/v1/config", params={"warehouse": name}, expected_code=404)
    response = catalog_request("GET", namespaces_url, expected_code=404)
    assert_error_shape(response, "NotFoundException")


def test_warehouse_role_is_used(started_cluster):
    # The mock STS returns bad keys for an unknown session name, so S3 refuses the write.
    name = f"badrole_{uuid.uuid4().hex[:8]}"
    credential = {**CREDENTIAL, "role_session_name": "unknown"}
    path = create_warehouse_in_keeper(name, f"s3://{BUCKET}/{name}", credential=credential)
    try:
        namespaces_url = f"/v1/{name}/namespaces"
        catalog_request("POST", namespaces_url, json={"namespace": ["ns"]})
        response = catalog_request(
            "POST", f"{namespaces_url}/ns/tables", json={"name": "t", "schema": DEFAULT_SCHEMA}, expected_code=500
        )
        assert_error_shape(response, "InternalServerError")
        assert list(cluster.minio_client.list_objects(BUCKET, f"{name}/", recursive=True)) == []
    finally:
        get_keeper().delete(path, recursive=True)


def test_bucket_root_base_location(started_cluster):
    # The warehouse of node_aux has `s3://warehouse/` as the base location, the bucket root with a trailing slash.
    # Tables land directly under the bucket.
    ns = f"root_{uuid.uuid4().hex[:8]}"
    ns_url = catalog_url("/v1/my_warehouse/namespaces", node_aux)
    assert requests.post(ns_url, json={"namespace": [ns]}).status_code == 200

    tables = catalog_url(tables_url(ns), node_aux)
    result = requests.post(tables, json={"name": "events", "schema": DEFAULT_SCHEMA})
    assert result.status_code == 200, result.text
    metadata = result.json()["metadata"]
    assert metadata["location"] == f"s3://{BUCKET}/{ns}/events-{metadata['table-uuid']}"


def test_auxiliary_keeper(started_cluster):
    # node_aux stores its state in the aux Keeper, which is chrooted to /aux_root.
    url = catalog_url("/v1/my_warehouse/namespaces", node_aux)
    assert requests.post(url, json={"namespace": ["aux_ns"]}).status_code == 200
    assert ["aux_ns"] in requests.get(url).json()["namespaces"]

    zk = get_keeper()
    assert zk.exists(f"/aux_root{KEEPER_ROOT}/namespaces/aux_ns")
    assert not zk.exists(f"{KEEPER_ROOT}/namespaces/aux_ns")


def test_secure_port(started_cluster):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    path = "/v1/config?warehouse=my_warehouse"
    secure_url = f"https://{node.ip_address}:{CATALOG_SECURE_PORT}{path}"

    response = requests.get(secure_url, auth=DEFAULT_AUTH, verify=False)
    assert response.status_code == 200
    assert response.json() == catalog_request("GET", path).json()

    # Authentication runs on the TLS listener too.
    response = requests.get(secure_url, auth=("default", "wrong"), verify=False)
    assert response.status_code == 401
    assert_error_shape(response, "NotAuthorizedException")

    # Plain HTTP to the TLS port is refused.
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get(
            f"http://{node.ip_address}:{CATALOG_SECURE_PORT}{path}", auth=DEFAULT_AUTH
        )
