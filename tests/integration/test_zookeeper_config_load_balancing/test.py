import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(
    __file__, zookeeper_config_path="configs/zookeeper_load_balancing.xml"
)

# use 3-letter hostnames, so getHostNameDifference("nod1", "zoo1") will work as expected
node1 = cluster.add_instance(
    "nod1", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing.xml"]
)
node2 = cluster.add_instance(
    "nod2", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing.xml"]
)
node3 = cluster.add_instance(
    "nod3", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing.xml"]
)


def change_balancing(old, new, reload=True):
    line = "<zookeeper_load_balancing>{}<"
    old_line = line.format(old)
    new_line = line.format(new)
    for node in [node1, node2, node3]:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/zookeeper_load_balancing.xml",
            old_line,
            new_line,
        )
        if reload:
            node.query("select '{}', '{}'".format(old, new))
            node.query("system reload config")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_first_or_random(started_cluster):
    try:
        change_balancing("random", "first_or_random")
        print(
            str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )
    finally:
        change_balancing("first_or_random", "random", reload=False)


def test_in_order(started_cluster):
    try:
        change_balancing("random", "in_order")
        print(
            str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )
    finally:
        change_balancing("in_order", "random", reload=False)


def test_nearest_hostname(started_cluster):
    try:
        change_balancing("random", "nearest_hostname")
        print(
            str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo1_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo2_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo3_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )
    finally:
        change_balancing("nearest_hostname", "random", reload=False)


def test_round_robin(started_cluster):
    pm = PartitionManager()
    try:
        pm._add_rule(
            {
                "source": node1.ip_address,
                "destination": cluster.get_instance_ip("zoo1"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        pm._add_rule(
            {
                "source": node2.ip_address,
                "destination": cluster.get_instance_ip("zoo1"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        pm._add_rule(
            {
                "source": node3.ip_address,
                "destination": cluster.get_instance_ip("zoo1"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        change_balancing("random", "round_robin")

        print(
            str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo2_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node2.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo2_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

        print(
            str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':2181' | grep ESTABLISHED",
                    ],
                    privileged=True,
                    user="root",
                )
            )
        )
        assert (
            "1"
            == str(
                node3.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep 'testzookeeperconfigloadbalancing_zoo2_1.*testzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )

    finally:
        pm.heal_all()
        change_balancing("round_robin", "random", reload=False)
