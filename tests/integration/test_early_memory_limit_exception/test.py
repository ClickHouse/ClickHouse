import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", main_configs=["configs/users_to_ignore_early_memory_limit_check.xml"]
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_enabling_access_management():
    instance.query("DROP USER IF EXISTS Alex")
    instance.query("CREATE USER Alex", user="default")

    # 100G should be probably enough
    instance.query("system allocate untracked memory 100000000000", user="default")
    instance.query("select 1", user="default")
    assert "(total) memory limit exceeded" in instance.query_and_get_error("select 1", user="readonly")

    server_ip = cluster.get_instance_ip("instance")
    endpoint = "'http://{}:8123/?query=SELECT+1&user=Alex'".format(server_ip)
    assert "(total) memory limit exceeded" in instance.exec_in_container(["bash", "-c", f"curl {endpoint}"])

    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/users_to_ignore_early_memory_limit_check.xml",
        "default",
        "default , Alex ",
    )

    instance.query("system reload config", user="default")

    assert "(total) memory limit exceeded" not in instance.exec_in_container(["bash", "-c", f"curl {endpoint}"])
    instance.query("select 1", user="default")

    assert "(total) memory limit exceeded" in instance.query_and_get_error("select 1", user="readonly")

    instance.query("system free untracked memory")
    instance.query("DROP USER IF EXISTS Alex")
