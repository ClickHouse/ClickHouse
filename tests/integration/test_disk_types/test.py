import pytest
from helpers.cluster import ClickHouseCluster

disk_types = {
    "default": "local",
    "disk_s3": "s3",
    "disk_memory": "memory",
    "disk_hdfs": "hdfs",
}


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", main_configs=["configs/storage.xml"], with_minio=True, with_hdfs=True)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_different_types(cluster):
    node = cluster.instances["node"]
    response = node.query("SELECT * FROM system.disks")
    disks = response.split("\n")
    for disk in disks:
        if disk == '':  # skip empty line (after split at last position)
            continue
        fields = disk.split("\t")
        assert len(fields) >= 6
        assert disk_types.get(fields[0], "UNKNOWN") == fields[5]


def test_select_by_type(cluster):
    node = cluster.instances["node"]
    for name, disk_type in list(disk_types.items()):
        assert node.query("SELECT name FROM system.disks WHERE type='" + disk_type + "'") == name + "\n"

