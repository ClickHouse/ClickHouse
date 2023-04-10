import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

disk_types = {
    "default": "local",
    "disk_s3": "s3",
    "disk_hdfs": "hdfs",
    "disk_encrypted": "s3",
}


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage.xml"],
            with_minio=True,
            with_hdfs=True,
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


def test_select_by_type(cluster):
    node = cluster.instances["node"]
    for name, disk_type in list(disk_types.items()):
        if disk_type != "s3":
            assert (
                node.query(
                    "SELECT name FROM system.disks WHERE type='" + disk_type + "'"
                )
                == name + "\n"
            )
        else:
            assert (
                node.query(
                    "SELECT name FROM system.disks WHERE type='"
                    + disk_type
                    + "' ORDER BY name"
                )
                == "disk_encrypted\ndisk_s3\n"
            )
