from helpers.cluster import ClickHouseCluster


def test_enable_username_access_type():
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
        node.query("GRANT SET DEFINER ON * TO foobar")
        assert (
                sorted(node.query(
                    "SHOW GRANTS FOR foobar"
                ).strip().split('\n'))
                == ["GRANT CREATE USER ON *.* TO foobar", "GRANT SET DEFINER ON * TO foobar"]
        )
        node.query("DROP USER foobar")
    finally:
        cluster.shutdown()
