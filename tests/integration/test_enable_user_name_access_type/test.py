from helpers.cluster import ClickHouseCluster


def test_startup_scripts():
    cluster = ClickHouseCluster(__file__)

    node = cluster.add_instance(
        "node",
        main_configs=[
            "configs/access_control_settings.xml",
        ],
        macros={"replica": "node", "shard": "node"},
        with_zookeeper=True,
    )

    try:
        cluster.start()
        node.query("CREATE USER foobar")
        node.query("GRANT CREATE USER ON * TO foobar")
        assert (
                node.query(
                    "SHOW GRANTS FOR foobar"
                )
                == "GRANT CREATE USER ON *.* TO foobar\n"
        )
        node.query("DROP USER foobar")
    finally:
        cluster.shutdown()
