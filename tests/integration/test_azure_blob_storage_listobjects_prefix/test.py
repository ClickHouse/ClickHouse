"""Regression test for the Azure ``listObjects`` endpoint-prefix bug (PR #112872).

On a prefixed-endpoint Azure Blob Storage disk, ``AzureObjectStorage::listObjects``
used to fetch pages 2..N with the SDK's ``MoveToNextPage()``, which bypasses
``ContainerClientWrapper``'s endpoint-prefix stripping, so every blob name after the
first page kept the raw Azure prefix (wrong paths).

The batched ``listObjects`` is reachable only through
``MetadataStorageFromPlainObjectStorage::listDirectory`` -- Azure overrides
``iterate()`` (``AzureIteratorAsync``, already correct), so plain_rewritable and
ordinary MergeTree disks never exercise the buggy path. This test therefore:

  * builds a MergeTree table with several wide parts on the default (local) disk,
  * uploads its ``store/<uuid>/`` tree into Azurite under a prefixed endpoint,
  * ATTACHes it read-only from a ``metadata_type=plain`` Azure disk with
    ``list_object_keys_size=1`` so every directory listing crosses page 1,
  * reads it back.

Pre-fix, pages 2..N come back with the raw endpoint prefix, so the parts are
garbled/broken and the read returns the wrong rows (or throws). Post-fix, every
page is stripped and the read returns all rows. Switching the code back to
``MoveToNextPage()`` makes this test fail again.
"""

import os

import pytest
from azure.storage.blob import BlobServiceClient

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import AzureUploader
from test_storage_azure_blob_storage.test import azure_query

NODE_NAME = "node"
ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)
CONTAINER_NAME = "cont"
# A non-empty path after the container name is what gives ContainerClientWrapper a
# prefix to strip -- without it there is nothing to reproduce.
DISK_PREFIX = "data/disks/disk_azure_plain/"
NUM_PARTS = 8


def generate_cluster_def(port):
    # Per-worker suffix so parallel xdist workers don't race on the generated file.
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
    suffix = f"_{worker_id}" if worker_id else ""
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f"./_gen/disk_storage_conf{suffix}.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(f"""<clickhouse>
    <storage_configuration>
        <disks>
            <disk_azure_plain_readonly>
                <type>object_storage</type>
                <object_storage_type>azure_blob_storage</object_storage_type>
                <metadata_type>plain</metadata_type>
                <endpoint>http://azurite1:{port}/{ACCOUNT_NAME}/{CONTAINER_NAME}/{DISK_PREFIX}</endpoint>
                <account_name>{ACCOUNT_NAME}</account_name>
                <account_key>{ACCOUNT_KEY}</account_key>
                <!-- One blob per page, so every listing crosses page 1. -->
                <list_object_keys_size>1</list_object_keys_size>
                <readonly>true</readonly>
            </disk_azure_plain_readonly>
        </disks>
        <policies>
            <azure_plain_readonly>
                <volumes>
                    <main>
                        <disk>disk_azure_plain_readonly</disk>
                    </main>
                </volumes>
            </azure_plain_readonly>
        </policies>
    </storage_configuration>
</clickhouse>
""")
    return path


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_cluster_def(port)
        cluster.add_instance(
            NODE_NAME,
            main_configs=[path],
            with_azurite=True,
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _blob_service_client(cluster):
    port = cluster.env_variables["AZURITE_PORT"]
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{port}/{ACCOUNT_NAME};"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def test_listobjects_prefix_crosses_page(cluster):
    node = cluster.instances[NODE_NAME]

    # 1. Build a MergeTree table with several separate wide parts on the local disk.
    node.query("CREATE DATABASE IF NOT EXISTS local_db")
    node.query(
        "CREATE TABLE local_db.src (num UInt32, data String) "
        "ENGINE=MergeTree ORDER BY num SETTINGS min_bytes_for_wide_part=0"
    )
    node.query("SYSTEM STOP MERGES local_db.src")
    for i in range(NUM_PARTS):
        node.query(f"INSERT INTO local_db.src VALUES ({i}, 'row{i}')")

    expected_count = NUM_PARTS
    expected_sum = sum(range(NUM_PARTS))
    assert int(node.query("SELECT count() FROM local_db.src")) == expected_count
    assert (
        int(
            node.query(
                "SELECT count() FROM system.parts "
                "WHERE database='local_db' AND table='src' AND active"
            )
        )
        == NUM_PARTS
    ), "each INSERT must stay a separate part so the directory listing crosses a page"

    table_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database='local_db' AND name='src'"
    ).strip()

    # 2. Upload the table's store/<uuid>/ tree into Azurite under the prefixed endpoint.
    #    Creating the container here is enough; the read-only disk never creates it.
    blob_service_client = _blob_service_client(cluster)
    try:
        blob_service_client.create_container(CONTAINER_NAME)
    except Exception:
        # Already exists on a re-run of the module -- fine.
        pass

    store_rel = os.path.join("store", table_uuid[:3], table_uuid)
    local_store_path = os.path.join(node.path, "database", store_rel)
    AzureUploader(
        blob_service_client, CONTAINER_NAME, use_relpath=True
    ).upload_directory(local_store_path, DISK_PREFIX + store_rel)

    # 3. Drop the local table; the data now lives only in Azurite.
    node.query("DROP TABLE local_db.src SYNC")

    # 4. Attach the same UUID read-only from the plain-metadata Azure disk.
    node.query("CREATE DATABASE IF NOT EXISTS azure_db")
    azure_query(
        node,
        f"ATTACH TABLE azure_db.dst UUID '{table_uuid}' (num UInt32, data String) "
        "ENGINE=MergeTree ORDER BY num "
        "SETTINGS storage_policy='azure_plain_readonly'",
        query_on_retry="DROP TABLE IF EXISTS azure_db.dst SYNC",
    )

    # 5. Reading loads the parts via MetadataStorageFromPlainObjectStorage::listDirectory
    #    -> AzureObjectStorage::listObjects, which paginates. Pre-fix, pages 2..N keep the
    #    raw endpoint prefix, so the parts are garbled/broken and the read is wrong (or throws).
    assert int(azure_query(node, "SELECT count() FROM azure_db.dst")) == expected_count
    assert int(azure_query(node, "SELECT sum(num) FROM azure_db.dst")) == expected_sum
    assert azure_query(
        node, "SELECT num, data FROM azure_db.dst ORDER BY num FORMAT Values"
    ) == ",".join(f"({i},'row{i}')" for i in range(NUM_PARTS))
