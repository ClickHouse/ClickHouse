import logging
import random
import string
import time

import pyarrow as pa
import pytest
from deltalake.writer import write_deltalake

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, azurite_default_port=10000)


def randomize_table_name(table_name, random_suffix_length=10):
    letters = string.ascii_letters + string.digits
    return f"{table_name}{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


def write_deltalake_with_retry(
    table_uri, data, storage_options, retries=3, delay=5, **kwargs
):
    last_exception = None
    for attempt in range(retries):
        try:
            write_deltalake(table_uri, data, storage_options=storage_options, **kwargs)
            return
        except OSError as e:
            last_exception = e
            if attempt < retries - 1:
                logging.warning(
                    f"write_deltalake failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {delay}s..."
                )
                time.sleep(delay)
    raise last_exception


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            with_azurite=True,
            stay_alive=True,
            with_zookeeper=True,
            macros={"shard": 0, "replica": 1},
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            with_azurite=True,
            stay_alive=True,
            with_zookeeper=True,
            macros={"shard": 0, "replica": 2},
        )

        logging.info("Starting cluster...")
        cluster.start()

        if (
            int(
                cluster.instances["node1"]
                .query(
                    "SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'"
                )
                .strip()
            )
            == 0
        ):
            pytest.skip("DeltaLake engine is not available")

        cluster.azure_container_name = "mycontainer"
        cluster.blob_service_client = cluster.blob_service_client
        container_client = cluster.blob_service_client.create_container(
            cluster.azure_container_name
        )
        cluster.container_client = container_client

        yield cluster

    finally:
        cluster.shutdown()


def test_cluster_function(started_cluster):
    instance = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_cluster_function")

    schema = pa.schema([("a", pa.int32()), ("b", pa.string())])
    data = [
        pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        pa.array(["aa", "bb", "cc", "aa", "bb"], type=pa.string()),
    ]

    storage_options = {
        "AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
        "AZURE_STORAGE_ACCOUNT_KEY": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "AZURE_STORAGE_CONTAINER_NAME": started_cluster.azure_container_name,
        "AZURE_STORAGE_USE_EMULATOR": "true",
    }
    path = f"abfss://{started_cluster.azure_container_name}@devstoreaccount1.dfs.core.windows.net/{table_name}"
    table = pa.Table.from_arrays(data, schema=schema)
    write_deltalake_with_retry(
        path, table, storage_options=storage_options, partition_by=["b"]
    )

    table_function = f"""
        deltaLakeAzureCluster(cluster, azure, container = '{started_cluster.azure_container_name}', storage_account_url = '{started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '{table_name}')
        """
    instance.query(f"SELECT * FROM {table_function} SETTINGS allow_experimental_analyzer=1")
    assert 5 == int(
        instance.query(
            f"SELECT count() FROM {table_function} SETTINGS allow_experimental_analyzer=1"
        )
    )
    assert "1\taa\n"
    "2\tbb\n"
    "3\tcc\n"
    "4\taa\n"
    "5\tbb\n" == instance.query(
        f"SELECT * FROM {table_function} ORDER BY a SETTINGS allow_experimental_analyzer=1"
    )
