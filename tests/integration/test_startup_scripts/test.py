from helpers.cluster import ClickHouseCluster


def test_startup_scripts():
    cluster = ClickHouseCluster(__file__)

    node = cluster.add_instance(
        "node",
        main_configs=[
            "configs/config.d/query_log.xml",
            "configs/config.d/startup_scripts.xml",
        ],
        macros={"replica": "node", "shard": "node"},
        with_zookeeper=True,
    )

    try:
        cluster.start()
        tables = node.query("SHOW TABLES")
        assert "TestTable" in tables
        assert "test_dict" in tables
        assert (
            node.query(
                "SELECT value, changed FROM system.settings WHERE name = 'skip_unavailable_shards'"
            )
            == "0\t0\n"
        )

        tables = node.query("SHOW TABLES FROM replicated")
        assert "test_replica" in tables
    finally:
        cluster.shutdown()
