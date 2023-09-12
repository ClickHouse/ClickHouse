import pytest
from helpers.cluster import ClickHouseCluster
import os

cluster = ClickHouseCluster(__file__, "test_migrate")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        instance = cluster.add_instance(
            'instance',
            base_config_dir="configs",
            user_configs=["configs/users.d/alice.xml"],
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start(destroy_dirs=True)

        yield cluster

    finally:
        cluster.shutdown()


def test_migrate_from_local(start_cluster):
    instance = start_cluster.instances["instance"]
    user_name = "test"
    instance.query(f"CREATE USER {user_name}")

    instance.stop_clickhouse()
    with open(os.path.dirname(__file__) + "/configs/config.d/foundationdb.xml", "r") as f:
        instance.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())

    instance.start_clickhouse()
    assert instance.query(f"SELECT count() FROM system.users WHERE name = '{user_name}'").strip() == "1"
