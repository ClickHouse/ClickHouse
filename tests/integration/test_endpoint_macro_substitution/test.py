import pytest

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.test_tools import TSV

disk_types = {
    "default": "Local",
    "disk_s3": "S3",
    "disk_encrypted": "S3",
}


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage.xml", "configs/macros.xml"],
            # Disable with_remote_database_disk to reduce diversion between the public and private repo.
            # So we do not handle the test differently in the private repo
            with_remote_database_disk=False,
            with_minio=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_select_by_type(cluster):
    node = cluster.instances["node"]
    for name, disk_type in list(disk_types.items()):
        if disk_type == "Local":
            assert (
                node.query(
                    "SELECT name FROM system.disks WHERE type='" + disk_type + "'"
                )
                == name + "\n"
            )
        elif disk_type == "S3":
            assert (
                node.query(
                    "SELECT name FROM system.disks WHERE object_storage_type='"
                    + disk_type
                    + "' ORDER BY name"
                )
                == "disk_encrypted\ndisk_s3\n"
            )
        else:
            assert (
                node.query(
                    "SELECT name FROM system.disks WHERE object_storage_type='"
                    + disk_type
                    + "'"
                )
                == name + "\n"
            )
