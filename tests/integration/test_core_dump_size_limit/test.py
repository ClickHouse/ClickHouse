import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/core_dump.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_core_dump_size_limit_hot_reload(start_cluster):
    def get_core_dump_limit():
        pid = node.get_process_pid("clickhouse")
        result = node.exec_in_container(
            ["bash", "-c", f"prlimit --pid {pid} --core --output=SOFT --noheadings"],
            user="root",
        )
        return int(result.strip())

    assert get_core_dump_limit() == 1073741824

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/core_dump.xml",
        "<size_limit>1073741824</size_limit>",
        "<size_limit>536870912</size_limit>",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert get_core_dump_limit() == 536870912

    # Restore original config so the test is repeatable
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/core_dump.xml",
        "<size_limit>536870912</size_limit>",
        "<size_limit>1073741824</size_limit>",
    )
    node.query("SYSTEM RELOAD CONFIG")
