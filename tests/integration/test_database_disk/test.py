import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/database_disk.xml"],
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_rename_database_with_database_disk(started_cluster):
    db_disk_path = node1.query(
        "SELECT path FROM system.disks WHERE name='custom_db_disk'"
    ).strip()

    def list_file_in_metadata_dir():
        return node1.exec_in_container(
            [
                "bash",
                "-c",
                f'ls "{db_disk_path}/metadata/"',
            ],
            privileged=True,
            user="root",
        )

    node1.query("DROP DATABASE IF EXISTS test SYNC")
    node1.query("CREATE DATABASE test ENGINE=Atomic")

    node1.query("RENAME DATABASE test TO test_rename")
    metadata_files = list_file_in_metadata_dir()
    assert "test.sql" not in metadata_files
    assert "test_rename.sql" in metadata_files

    node1.query("RENAME DATABASE test_rename TO test")
    metadata_files = list_file_in_metadata_dir()
    assert "test.sql" in metadata_files
    assert "test_rename.sql" not in metadata_files

    node1.query("DROP DATABASE IF EXISTS test SYNC")
