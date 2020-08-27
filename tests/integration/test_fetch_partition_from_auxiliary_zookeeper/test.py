from __future__ import print_function
from helpers.cluster import ClickHouseCluster
import helpers
import pytest

def test_chroot_with_same_root():

    cluster = ClickHouseCluster(
        __file__, zookeeper_config_path="configs/zookeeper_config.xml"
    )

    node = cluster.add_instance(
        "node", config_dir="configs", with_zookeeper=True, zookeeper_use_tmpfs=False
    )

    try:
        cluster.start()

        node.query(
            "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'node') PARTITION BY date;"
        )
        node.query("INSERT INTO simple VALUES ('2020-08-27', 1)")

        node.query(
            "CREATE TABLE simple2 (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/simple', 'node') PARTITION BY date;"
        )
        node.query(
            "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zookeeper2:/clickhouse/tables/0/simple';"
        )
        node.query("ALTER TABLE simple2 ATTACH PARTITION '2020-08-27';")

        assert node.query("SELECT id FROM simple2").strip() == "1"

    finally:
        cluster.shutdown()
