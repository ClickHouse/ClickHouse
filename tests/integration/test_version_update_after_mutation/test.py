import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="20.1.10.70",
    with_installed_binary=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="20.1.10.70",
    with_installed_binary=True,
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="20.1.10.70",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_mutate_and_upgrade(start_cluster):
    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS mt")
        node.query(
            "CREATE TABLE mt (EventDate Date, id UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/t', '{}') ORDER BY tuple()".format(
                node.name
            )
        )

    node1.query("INSERT INTO mt VALUES ('2020-02-13', 1), ('2020-02-13', 2);")

    node1.query("ALTER TABLE mt DELETE WHERE id = 2", settings={"mutations_sync": "2"})
    node2.query("SYSTEM SYNC REPLICA mt", timeout=15)

    node2.query("DETACH TABLE mt")  # stop being leader
    node1.query("DETACH TABLE mt")  # stop being leader
    node1.restart_with_latest_version(signal=9)
    node2.restart_with_latest_version(signal=9)

    # After hard restart table can be in readonly mode
    exec_query_with_retry(
        node2, "INSERT INTO mt VALUES ('2020-02-13', 3)", retry_count=60
    )
    exec_query_with_retry(node1, "SYSTEM SYNC REPLICA mt", retry_count=60)

    assert node1.query("SELECT COUNT() FROM mt") == "2\n"
    assert node2.query("SELECT COUNT() FROM mt") == "2\n"

    node1.query("INSERT INTO mt VALUES ('2020-02-13', 4);")

    node2.query("SYSTEM SYNC REPLICA mt", timeout=15)

    assert node1.query("SELECT COUNT() FROM mt") == "3\n"
    assert node2.query("SELECT COUNT() FROM mt") == "3\n"

    node2.query("ALTER TABLE mt DELETE WHERE id = 3", settings={"mutations_sync": "2"})

    node1.query("SYSTEM SYNC REPLICA mt", timeout=15)

    assert node1.query("SELECT COUNT() FROM mt") == "2\n"
    assert node2.query("SELECT COUNT() FROM mt") == "2\n"

    node1.query(
        "ALTER TABLE mt MODIFY COLUMN id Int32 DEFAULT 0",
        settings={"replication_alter_partitions_sync": "2"},
    )

    node2.query("OPTIMIZE TABLE mt FINAL")

    assert node1.query("SELECT id FROM mt") == "1\n4\n"
    assert node2.query("SELECT id FROM mt") == "1\n4\n"

    for node in [node1, node2]:
        node.query("DROP TABLE mt")


def test_upgrade_while_mutation(start_cluster):
    node3.query("DROP TABLE IF EXISTS mt1")

    node3.query(
        "CREATE TABLE mt1 (EventDate Date, id UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/t1', 'node3') ORDER BY tuple()"
    )

    node3.query("INSERT INTO mt1 select '2020-02-13', number from numbers(100000)")

    node3.query("SYSTEM STOP MERGES mt1")
    node3.query("ALTER TABLE mt1 DELETE WHERE id % 2 == 0")

    node3.query("DETACH TABLE mt1")  # stop being leader
    node3.restart_with_latest_version(signal=9)

    # checks for readonly
    exec_query_with_retry(node3, "OPTIMIZE TABLE mt1", sleep_time=5, retry_count=60)

    node3.query(
        "ALTER TABLE mt1 DELETE WHERE id > 100000", settings={"mutations_sync": "2"}
    )
    # will delete nothing, but previous async mutation will finish with this query

    assert_eq_with_retry(node3, "SELECT COUNT() from mt1", "50000\n")

    node3.query("DROP TABLE mt1")
