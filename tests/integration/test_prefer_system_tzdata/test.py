import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["config.d/prefer_system_tz.xml",], stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_prefer_system_tzdata(start_cluster):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"rm /usr/share/zoneinfo/Africa/Tunis && ln -s /usr/share/zoneinfo/America/New_York /usr/share/zoneinfo/Africa/Tunis",
        ],
        privileged=True,
    )

    node.restart_clickhouse()

    assert node.exec_in_container([f"bash", "-c", f"echo \"SELECT toDateTime(toDateTime('2024-05-01 12:12:12', 'UTC'), 'Africa/Tunis')\" | curl -s {node.hostname}:8123/ --data-binary @-"]) == "2024-05-01 08:12:12\n"
