import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

MOCK_PORT = 8083
NUM_ROWS = 100

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance("node", with_minio=True, stay_alive=True)
    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.join(os.path.dirname(__file__), "s3_mocks"),
            [("empty_list_page.py", "resolver", str(MOCK_PORT))],
        )
        yield cluster
    finally:
        cluster.shutdown()


def control_mock(command):
    response = cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", f"http://localhost:{MOCK_PORT}/{command}"],
    )
    assert response == "OK", f"unexpected reply from the mock: {response}"


def test_empty_listing_page_does_not_hide_parts(start_cluster):
    """An empty `ListObjectsV2` page must not make a `plain_rewritable` disk look empty.

    `MetadataStorageFromPlainRewritableObjectStorage::load` decides whether the disk holds
    anything with `existsOrHasAnyChild`, which lists with `max-keys=1`. If that listing stops at
    an empty page instead of following the continuation token, the disk comes up with no
    directories at all and the table silently loses every part it has.

    The disk is defined on the table rather than in the server config so that it is created
    after the mock proxy is listening; it is then recreated on every subsequent startup.
    """
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_empty_page SYNC")
    node.query(
        f"""
        CREATE TABLE test_empty_page (key Int32, value String)
        ENGINE = MergeTree()
        ORDER BY key
        SETTINGS disk = disk(
            name = disk_empty_page,
            type = s3_plain_rewritable,
            endpoint = 'http://resolver:{MOCK_PORT}/root/data/',
            access_key_id = minio,
            secret_access_key = 'ClickHouse_Minio_P@ssw0rd')
        """
    )
    node.query(
        f"INSERT INTO test_empty_page SELECT number, toString(number) FROM numbers({NUM_ROWS})"
    )

    assert int(node.query("SELECT count() FROM test_empty_page")) == NUM_ROWS

    # Arm while the server is down, so the disk load during startup is what consumes the injected
    # page rather than some background listing that happens to run first. From now on the first
    # listing of each prefix answers with no keys, `IsTruncated=true` and a continuation token.
    node.stop_clickhouse()
    control_mock("arm")
    node.start_clickhouse()

    assert int(node.query("SELECT count() FROM test_empty_page")) == NUM_ROWS
    assert (
        int(
            node.query(
                "SELECT count() FROM system.parts "
                "WHERE database = 'default' AND table = 'test_empty_page' AND active"
            )
        )
        > 0
    )

    control_mock("disarm")
    node.query("DROP TABLE test_empty_page SYNC")


def test_empty_listing_page_does_not_create_intersecting_parts(start_cluster):
    """A disk that comes up empty makes the table renumber its blocks and collide with itself.

    This is the damage the previous test stops one step short of. If a restart hides the existing
    parts, the block counter restarts from 1, so parts written afterwards reuse block numbers that
    are already on the disk. Each generation merges into a level 1 part sharing the same left
    edge - `all_1_2_1` and `all_1_3_1` - and `MergeTreePartInfo::contains` rejects that pair,
    because a containing part may only have an equal level when the block range is identical.
    Once both generations are visible the table cannot be loaded at all:
    `Part ... intersects previous part ...`.

    The merges matter: without them both generations would only produce level 0 parts whose names
    collide outright, which is not an intersection.
    """
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_intersecting SYNC")
    node.query(
        f"""
        CREATE TABLE test_intersecting (key Int32, value String)
        ENGINE = MergeTree()
        ORDER BY key
        SETTINGS disk = disk(
            name = disk_intersecting,
            type = s3_plain_rewritable,
            endpoint = 'http://resolver:{MOCK_PORT}/root/intersecting/',
            access_key_id = minio,
            secret_access_key = 'ClickHouse_Minio_P@ssw0rd')
        """
    )

    def insert(count, offset):
        node.query(
            f"INSERT INTO test_intersecting "
            f"SELECT number + {offset}, toString(number) FROM numbers({count})"
        )

    # First generation: two parts merged into all_1_2_1.
    insert(50, 0)
    insert(50, 50)
    node.query("OPTIMIZE TABLE test_intersecting FINAL")
    assert int(node.query("SELECT count() FROM test_intersecting")) == 100

    node.stop_clickhouse()
    control_mock("arm")
    node.start_clickhouse()

    # Second generation. Without the fix the table is empty here, so these blocks are numbered
    # from 1 again and the merge below produces all_1_3_1 next to the surviving all_1_2_1.
    insert(50, 100)
    insert(50, 150)
    insert(50, 200)
    node.query("OPTIMIZE TABLE test_intersecting FINAL")

    # Restore normal listings, so the next startup sees every directory on the disk.
    node.stop_clickhouse()
    control_mock("disarm")
    node.start_clickhouse()

    assert int(node.query("SELECT count() FROM test_intersecting")) == 250
    assert not node.contains_in_log("intersects previous part")
    assert not node.contains_in_log("intersects next part")

    node.query("DROP TABLE test_intersecting SYNC")
