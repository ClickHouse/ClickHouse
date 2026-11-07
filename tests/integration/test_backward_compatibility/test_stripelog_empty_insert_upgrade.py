import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# 26.3 records an empty object for an empty INSERT into a StripeLog table on object storage and reads the table fine.
node = cluster.add_instance(
    "node",
    main_configs=["configs/stripelog_object_storage.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.3.42.3",
    with_installed_binary=True,
    with_minio=True,
    stay_alive=True,
)

POLICIES = ["stripelog_s3", "stripelog_s3_cache"]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_stripelog_with_empty_insert_is_readable_after_upgrade(started_cluster):
    for policy in POLICIES:
        node.query(
            f"CREATE TABLE t_{policy} (n UInt32) ENGINE = StripeLog SETTINGS storage_policy = '{policy}'"
        )
        node.query(f"INSERT INTO t_{policy} SELECT 1")
        node.query(f"INSERT INTO t_{policy} SELECT 1 LIMIT 0")
        node.query(f"INSERT INTO t_{policy} SELECT 2")
        # The old version lists an empty object in the data file.
        assert (
            node.query(
                "SELECT uniqExactIf(remote_path, size = 0) FROM system.remote_data_paths "
                f"WHERE local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE name = 't_{policy}'), '/data.bin')"
            )
            == "1\n"
        )
        assert node.query(f"SELECT n FROM t_{policy} ORDER BY n") == "1\n2\n"

    node.restart_with_latest_version()

    for policy in POLICIES:
        for settings in (
            {},
            {"max_threads": 1},
            {"remote_filesystem_read_method": "read"},
        ):
            assert (
                node.query(f"SELECT n FROM t_{policy} ORDER BY n", settings=settings)
                == "1\n2\n"
            )
