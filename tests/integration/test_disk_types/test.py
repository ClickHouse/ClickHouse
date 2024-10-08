import pytest

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.test_tools import TSV

disk_types = {
    "default": "Local",
    "disk_s3": "S3",
    "disk_encrypted": "S3",
}

# do not test HDFS on ARM
if not is_arm():
    disk_types["disk_hdfs"] = "HDFS"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=(
                ["configs/storage_arm.xml"] if is_arm() else ["configs/storage_amd.xml"]
            ),
            with_minio=True,
            with_hdfs=not is_arm(),
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_different_types(cluster):
    node = cluster.instances["node"]
    response = TSV.toMat(node.query("SELECT * FROM system.disks FORMAT TSVWithNames"))

    assert len(response) > len(disk_types)  # at least one extra line for header

    name_col_ix = response[0].index("name")
    type_col_ix = response[0].index("type")
    encrypted_col_ix = response[0].index("is_encrypted")

    for fields in response[1:]:  # skip header
        assert len(fields) >= 7
        assert (
            disk_types.get(fields[name_col_ix], "UNKNOWN") == fields[type_col_ix]
        ), f"Wrong type ({fields[type_col_ix]}) for disk {fields[name_col_ix]}!"
        if "encrypted" in fields[name_col_ix]:
            assert (
                fields[encrypted_col_ix] == "1"
            ), f"{fields[name_col_ix]} expected to be encrypted!"
        else:
            assert (
                fields[encrypted_col_ix] == "0"
            ), f"{fields[name_col_ix]} expected to be non-encrypted!"


def test_different_types(cluster):
    node = cluster.instances["node"]
    response = TSV.toMat(node.query("SELECT * FROM system.disks FORMAT TSVWithNames"))

    assert len(response) > len(disk_types)  # at least one extra line for header

    name_col_ix = response[0].index("name")
    type_col_ix = response[0].index("type")
    encrypted_col_ix = response[0].index("is_encrypted")

    for fields in response[1:]:  # skip header
        assert len(fields) >= 7
        expected_disk_type = disk_types.get(fields[name_col_ix], "UNKNOWN")

        if expected_disk_type != "Local":
            disk_type = fields[response[0].index("object_storage_type")]
        else:
            disk_type = fields[type_col_ix]

        assert (
            expected_disk_type == disk_type
        ), f"Wrong type ({fields[type_col_ix]}) for disk {fields[name_col_ix]}!"
        if "encrypted" in fields[name_col_ix]:
            assert (
                fields[encrypted_col_ix] == "1"
            ), f"{fields[name_col_ix]} expected to be encrypted!"
        else:
            assert (
                fields[encrypted_col_ix] == "0"
            ), f"{fields[name_col_ix]} expected to be non-encrypted!"


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
