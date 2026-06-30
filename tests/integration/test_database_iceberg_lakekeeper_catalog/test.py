import requests
import time
import uuid

import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
)

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.test_tools import TSV, csv_compare

BASE_URL = "http://lakekeeper:8181/catalog"
CATALOG_NAME = "demo"
WAREHOUSE_NAME = "demo"


def get_lakekeeper_local_url(cluster):
    return f"http://localhost:{cluster.iceberg_rest_catalog_port}"

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\n(\n    `id` Nullable(Float64),\n    `data` Nullable(String)\n)\nENGINE = Iceberg('http://minio1:9001/warehouse-rest/data/', 'minio', '[HIDDEN]')\n"


def create_warehouse(cluster, minio_ip, minio_port):
    minio_endpoint = f"http://{minio_ip}:{minio_port}"

    warehouse_data = {
        "warehouse-name": "demo",
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse-rest",
            "key-prefix": "",
            "assume-role-arn": None,
            "endpoint": minio_endpoint,
            "region": "local-01",
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": True
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "minio",
            "aws-secret-access-key": "ClickHouse_Minio_P@ssw0rd"
        }
    }

    try:
        response = requests.post(
            f"{get_lakekeeper_local_url(cluster)}/management/v1/warehouse",
            headers={"Content-Type": "application/json"},
            json=warehouse_data,
            timeout=30
        )

        if response.status_code == 201:
            pass
        elif response.status_code == 409:
            pass
        else:
            response.raise_for_status()

    except requests.exceptions.RequestException:
        raise


def load_catalog_impl(started_cluster):
    minio_ip = started_cluster.minio_ip
    minio_port = started_cluster.minio_port
    s3_endpoint = f"http://{minio_ip}:{minio_port}"

    return RestCatalog(
        name="my_catalog",
        warehouse=WAREHOUSE_NAME,
        uri=f"{get_lakekeeper_local_url(started_cluster)}/catalog",
        token="dummy",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
            extra_parameters={
                "docker_compose_file_name": "docker_compose_iceberg_lakekeeper_catalog.yml"
            },
        )

        cluster.start()

        time.sleep(15)

        minio_ip = cluster.minio_ip
        minio_port = cluster.minio_port
        create_warehouse(cluster, minio_ip, minio_port)

        yield cluster

    finally:
        cluster.shutdown()

def test_list_tables(started_cluster):
    node = started_cluster.instances["node1"]

    namespace_prefix = f"clickhouse_{uuid.uuid4().hex[:8]}"
    namespace_1 = f"{namespace_prefix}_testA"
    namespace_2 = f"{namespace_prefix}_testB"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace((namespace,))

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables((namespace,))) == 0

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    tables_list = ""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    for table in namespace_1_tables:
        catalog.create_table(
            (namespace_1, table),
            schema=schema,
            properties={"write.metadata.compression-codec": "none"},
        )
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_1}.{table}"

    for table in namespace_2_tables:
        catalog.create_table(
            (namespace_2, table),
            schema=schema,
            properties={"write.metadata.compression-codec": "none"},
        )
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_2}.{table}"

    # Verify tables were created via PyIceberg
    assert len(catalog.list_tables((namespace_1,))) == 2
    assert len(catalog.list_tables((namespace_2,))) == 2

    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{namespace_prefix}%' ORDER BY name SETTINGS show_data_lake_catalogs_in_system_tables = true"
        ).strip()
    )


def test_select(started_cluster):
    """Test select operations with Lakekeeper catalog"""

    node = started_cluster.instances["node1"]

    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_select_{uuid.uuid4().hex[:8]}"
    test_namespace = (f"{test_ref}_namespace",)
    existing_namespaces = catalog.list_namespaces()

    if test_namespace not in existing_namespaces:
        catalog.create_namespace(test_namespace)

    test_table_name = f"{test_ref}_table"
    test_table_identifier = test_namespace + (test_table_name,)

    try:
        existing_tables = catalog.list_tables(namespace=test_namespace)

        if test_table_identifier in existing_tables:
            catalog.drop_table(test_table_identifier)
    except Exception:
        pass

    simple_schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    table = catalog.create_table(
        test_table_identifier,
        schema=simple_schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    df = pd.DataFrame(
        {
            "id": [1.0, 2.0, 3.0, 4.0, 5.0],
            "data": ["hello", "world", "from", "lakekeeper", "test"],
        }
    )

    pa_df = pa.Table.from_pandas(df)

    table.append(pa_df)

    scan_result = table.scan().to_pandas()

    assert len(scan_result) == 5
    assert list(scan_result["id"]) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert list(scan_result["data"]) == ["hello", "world", "from", "lakekeeper", "test"]

    catalog.list_namespaces()

    catalog.list_tables(namespace=test_namespace)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    assert int(node.query(f"SELECT count(*) FROM {CATALOG_NAME}.`{test_namespace[0]}.{test_table_name}`")) == len(scan_result)

    result = node.query(f"SELECT id, data FROM {CATALOG_NAME}.`{test_namespace[0]}.{test_table_name}` ORDER BY id FORMAT TSV")
    expected = TSV("""
1   hello
2	world
3	from
4	lakekeeper
5	test
""")
    assert csv_compare(result, expected), f"got\n{result}\nwant\n{expected}"




def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_experimental_database_iceberg=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result


def test_hide_sensitive_info(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_hide_sensitive_info_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = (root_namespace,)
    catalog = load_catalog_impl(started_cluster)

    existing_namespaces = catalog.list_namespaces()
    if namespace not in existing_namespaces:
        catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    catalog.create_table(
        namespace + (table_name,),
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    def check_secret_hidden(secret, additional_settings):
        settings = {
            "catalog_type": "rest",
            "warehouse": "demo",
            "storage_endpoint": "http://minio1:9001/warehouse-rest",
        }
        settings.update(additional_settings)

        node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME}")
        try:
            node.query(
                f"""SET allow_experimental_database_iceberg=true;
CREATE DATABASE {CATALOG_NAME} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k + "=" + repr(v) for k, v in settings.items()))}"""
            )
        except QueryRuntimeException as e:
            message = str(e).split("\n(query:")[0]
            assert secret not in message, (
                f"Secret {secret!r} leaked into CREATE DATABASE error message"
            )
            assert minio_secret_key not in message, (
                "minio secret key leaked into CREATE DATABASE error message"
            )
            return

        show_result = node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")
        assert secret not in show_result
        assert minio_secret_key not in show_result

    check_secret_hidden("SECRET_1", {"catalog_credential": "id:SECRET_1"})
    check_secret_hidden("SECRET_2", {"auth_header": "Authorization: SECRET_2"})

def test_tables_with_same_location(started_cluster):

    node = started_cluster.instances["node1"]

    test_ref = f"test_tables_with_same_location_{uuid.uuid4().hex[:8]}"
    namespace = (f"{test_ref}_namespace",)
    catalog = load_catalog_impl(started_cluster)

    table_name = f"{test_ref}_table"
    table_name_2 = f"{test_ref}_table_2"

    existing_namespaces = catalog.list_namespaces()
    if namespace not in existing_namespaces:
        catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
    )
    table = catalog.create_table(
        namespace + (table_name,),
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )
    table_2 = catalog.create_table(
        namespace + (table_name_2,),
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    df1 = pd.DataFrame({"id": [1.0, 2.0, 3.0], "symbol": ["aaa", "aaa", "aaa"]})
    df2 = pd.DataFrame({"id": [1.0, 2.0, 3.0], "symbol": ["bbb", "bbb", "bbb"]})

    table.append(pa.Table.from_pandas(df1))
    table_2.append(pa.Table.from_pandas(df2))

    scan_result_1 = table.scan().to_pandas()
    scan_result_2 = table_2.scan().to_pandas()
    assert len(scan_result_1) == 3
    assert len(scan_result_2) == 3

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    assert 'aaa\naaa\naaa' == node.query(
        f"SELECT symbol FROM {CATALOG_NAME}.`{namespace[0]}.{table_name}`"
    ).strip()
    assert 'bbb\nbbb\nbbb' == node.query(
        f"SELECT symbol FROM {CATALOG_NAME}.`{namespace[0]}.{table_name_2}`"
    ).strip()


def test_invalid_auth_header_format(started_cluster):
    node = started_cluster.instances["node1"]

    node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME};")
    with pytest.raises(Exception) as err:
        node.query(
            f"""
            SET allow_experimental_database_iceberg = 1;
            CREATE DATABASE {CATALOG_NAME}
            ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', 'dummy')
            SETTINGS
                catalog_type = 'rest',
                warehouse = 'demo',
                auth_header = 'wrong.header'
            """
        )
    assert "Invalid auth header format" in str(err.value)


def get_credentials_profile_events(node, query_id):
    node.query("SYSTEM FLUSH LOGS")
    vended = int(node.query(
        f"SELECT ProfileEvents['DataLakeRestCatalogCredentialsVended'] "
        f"FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    ))
    hits = int(node.query(
        f"SELECT ProfileEvents['DataLakeRestCatalogCredentialsCacheHits'] "
        f"FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    ))
    return vended, hits


def test_vended_credentials_cache(started_cluster):
    node = started_cluster.instances["node1"]
    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_vended_credentials_cache_{uuid.uuid4().hex[:8]}"
    namespace = (f"{test_ref}_namespace",)
    table_name = f"{test_ref}_table"
    db_name = f"{test_ref}_database"

    if namespace not in catalog.list_namespaces():
        catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = catalog.create_table(
        namespace + (table_name,),
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )
    table.append(
        pa.Table.from_pandas(
            pd.DataFrame({"id": [1], "data": ["x"]}).astype({"id": "int32"})
        )
    )

    query = f"SELECT count() FROM {db_name}.`{namespace[0]}.{table_name}`"

    # Caching enabled (default TTL): the second query reuses cached credentials
    # and does not ask the catalog to vend them again.
    create_clickhouse_iceberg_database(started_cluster, node, db_name)

    qid = f"{test_ref}-cache-1-{uuid.uuid4()}"
    node.query(query, query_id=qid)
    vended, _ = get_credentials_profile_events(node, qid)
    assert vended >= 1

    qid = f"{test_ref}-cache-2-{uuid.uuid4()}"
    node.query(query, query_id=qid)
    vended, hits = get_credentials_profile_events(node, qid)
    assert vended == 0 and hits >= 1

    # Caching disabled (TTL = 0): every query asks the catalog to vend credentials.
    create_clickhouse_iceberg_database(
        started_cluster, node, db_name,
        additional_settings={"vended_credentials_cache_ttl": 0},
    )

    qid = f"{test_ref}-nocache-1-{uuid.uuid4()}"
    node.query(query, query_id=qid)
    vended, hits = get_credentials_profile_events(node, qid)
    assert vended >= 1 and hits == 0

    qid = f"{test_ref}-nocache-2-{uuid.uuid4()}"
    node.query(query, query_id=qid)
    vended, hits = get_credentials_profile_events(node, qid)
    assert vended >= 1 and hits == 0


def test_vended_credentials_cache_invalidated_on_table_replace(started_cluster):
    node = started_cluster.instances["node1"]
    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_vended_credentials_cache_replace_{uuid.uuid4().hex[:8]}"
    namespace = (f"{test_ref}_namespace",)
    table_name = f"{test_ref}_table"
    db_name = f"{test_ref}_database"

    if namespace not in catalog.list_namespaces():
        catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    def create_and_fill(rows):
        table = catalog.create_table(
            namespace + (table_name,),
            schema=schema,
            properties={"write.metadata.compression-codec": "none"},
        )
        table.append(
            pa.Table.from_pandas(
                pd.DataFrame({"id": list(range(rows)), "data": ["x"] * rows}).astype({"id": "int32"})
            )
        )

    create_and_fill(1)
    create_clickhouse_iceberg_database(started_cluster, node, db_name)
    query = f"SELECT count() FROM {db_name}.`{namespace[0]}.{table_name}`"

    # Populate the cache, then confirm the next query reuses it.
    node.query(query, query_id=f"{test_ref}-1-{uuid.uuid4()}")
    qid = f"{test_ref}-2-{uuid.uuid4()}"
    node.query(query, query_id=qid)
    vended, hits = get_credentials_profile_events(node, qid)
    assert vended == 0 and hits >= 1

    # Replace the table (new UUID and location) while the cache entry is still valid.
    catalog.drop_table(namespace + (table_name,))
    create_and_fill(2)

    # The stale entry must be detected, so credentials are re-vended and the new table is read.
    qid = f"{test_ref}-3-{uuid.uuid4()}"
    assert node.query(query, query_id=qid).strip() == "2"
    vended, _ = get_credentials_profile_events(node, qid)
    assert vended >= 1

    # The re-vended credentials are cached under the new identity, so the next query reuses them.
    qid = f"{test_ref}-4-{uuid.uuid4()}"
    node.query(query, query_id=qid)
    vended, hits = get_credentials_profile_events(node, qid)
    assert vended == 0 and hits >= 1

