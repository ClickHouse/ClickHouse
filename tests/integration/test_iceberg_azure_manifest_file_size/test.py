import logging
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

AZURE_CONTAINER = "testcontainer"

# `azure_min_upload_part_size` defaults to 16 MiB, so the data file has to be comfortably larger
# than one upload part for the last partial part to matter. Random strings keep Parquet from
# compressing the data away.
ROWS = 1500000


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            with_azurite=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        container_client = cluster.blob_service_client.get_container_client(
            AZURE_CONTAINER
        )
        if not container_client.exists():
            container_client.create_container()

        yield cluster
    finally:
        cluster.shutdown()


def blob_sizes(cluster, blob_path):
    container = cluster.blob_service_client.get_container_client(AZURE_CONTAINER)
    return {
        blob.name.split("/")[-1]: blob.size
        for blob in container.list_blobs(name_starts_with=f"{blob_path}data/")
    }


def test_manifest_file_size_matches_blob_size(started_cluster):
    """`file_size_in_bytes` must be the real blob size.

    ClickHouse only uses it as a hint, but Iceberg readers such as Spark take it as the length of
    the file and look for the Parquet footer at `file_size_in_bytes - 8`, so a value that is short
    by the trailing upload part makes the file unreadable outside of ClickHouse.
    """
    node = started_cluster.instances["node1"]
    azurite_url = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    table = "t_" + uuid.uuid4().hex[:8]
    blob_path = f"iceberg_manifest_file_size/{table}/"

    node.query(
        f"""
        CREATE TABLE {table} (x UInt64, s String) ENGINE = IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        """
    )

    node.query(
        f"INSERT INTO {table} SELECT number, randomPrintableASCII(40) FROM numbers({ROWS})",
        settings={
            "allow_insert_into_iceberg": 1,
            "max_insert_threads": 1,
            "iceberg_insert_max_rows_in_data_file": ROWS,
        },
    )

    assert node.query(f"SELECT count() FROM {table}").strip() == str(ROWS)

    real_sizes = blob_sizes(started_cluster, blob_path)
    declared = node.query(
        f"""
        SELECT splitByChar('/', file_path)[-1], file_size_in_bytes
        FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '{table}'
        FORMAT TSV
        """
    ).splitlines()
    assert declared, "no data files in the manifest"

    mismatches = []
    checked_over_one_part = 0
    for line in declared:
        name, size = line.split("\t")
        size = int(size)
        real = real_sizes[name]
        if real > 16 * 1024 * 1024:
            checked_over_one_part += 1
        if size != real:
            mismatches.append((name, size, real, real - size))

    assert checked_over_one_part > 0, (
        f"the data file did not exceed one upload part, sizes: {real_sizes}"
    )
    assert not mismatches, f"manifest sizes differ from the real blobs: {mismatches}"
