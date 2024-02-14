import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(
    __file__, zookeeper_config_path="configs/fdbkeeper_load_balancing.xml"
)

# use 3-letter hostnames, so getHostNameDifference("nod1", "zoo1") will work as expected
node1 = cluster.add_instance(
    "nod1", with_zookeeper=True, main_configs=["configs/fdbkeeper_load_balancing.xml"]
)
node2 = cluster.add_instance(
    "nod2", with_zookeeper=True, main_configs=["configs/fdbkeeper_load_balancing.xml"]
)
node3 = cluster.add_instance(
    "nod3", with_zookeeper=True, main_configs=["configs/fdbkeeper_load_balancing.xml"]
)


def change_balancing(old, new, reload=True):
    line = "<zookeeper_load_balancing>{}<"
    old_line = line.format(old)
    new_line = line.format(new)
    for node in [node1, node2, node3]:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/fdbkeeper_load_balancing.xml",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )
    finally:
        change_balancing("nearest_hostname", "random", reload=False)


def test_hostname_levenshtein_distance(started_cluster):
    try:
        change_balancing("random", "hostname_levenshtein_distance")
        print(
            str(
                node1.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
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
                        "lsof -a -i4 -i6 -itcp -w | grep ':4501' | grep ESTABLISHED",
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
                        "lsof -a -i4 -i6 -itcp -w | grep 'testfdbkeeperconfigloadbalancing_foundationdb1_1.*testfdbkeeperconfigloadbalancing_default:4501' | grep ESTABLISHED | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            ).strip()
        )
    finally:
        change_balancing("hostname_levenshtein_distance", "random", reload=False)
