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


def test_auth_method_grants_not_widened_when_user_name_access_type_disabled():
    # With `access_control_improvements.enable_user_name_access_type = 0`, a plain `GRANT ALTER USER ON alice`
    # is dumped as `ALTER USER ON *.*` for backward compatibility with older replicas. A per-authentication-method
    # `GRANTS (...)` clause must NOT be widened the same way: it is a fail-close credential limit, and older
    # replicas cannot parse the clause at all, so there is no compatibility to preserve. Widening it would broaden
    # a narrow token after `SHOW CREATE USER`, backup, restart, or `ATTACH USER`.
    cluster = ClickHouseCluster(__file__)

    node = cluster.add_instance(
        "node_grants",
        main_configs=[
            "configs/access_control_settings.xml",
        ],
        stay_alive=True,
    )

    try:
        cluster.start()

        node.query("CREATE USER alice")

        # Sanity: the regular grant path is still widened (backward compatibility is preserved where it applies).
        node.query("GRANT ALTER USER ON alice TO alice")
        assert (
            node.query("SHOW GRANTS FOR alice").strip()
            == "GRANT ALTER USER ON *.* TO alice"
        )

        node.query(
            "CREATE USER tok IDENTIFIED WITH no_password GRANTS (ALTER USER ON alice)"
        )

        def check_precise():
            create = node.query("SHOW CREATE USER tok")
            assert "GRANTS (ALTER USER ON alice)" in create, create
            assert "*.*" not in create, create
            auth_grants = node.query(
                "SELECT arrayJoin(auth_grants) FROM system.users WHERE name = 'tok'"
            ).strip()
            assert auth_grants == "ALTER USER ON alice", auth_grants

        # `SHOW CREATE USER` and `system.users.auth_grants` render precisely ...
        check_precise()

        # ... and the on-disk serialization survives a restart (the `ATTACH USER` round-trip) without widening.
        node.restart_clickhouse()
        check_precise()

        node.query("DROP USER tok, alice")
    finally:
        cluster.shutdown()
