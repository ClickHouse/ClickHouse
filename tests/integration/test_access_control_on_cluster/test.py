import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    with_zookeeper=True,
)
ch3 = cluster.add_instance(
    "ch3",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    with_zookeeper=True,
)
# `ch4` runs in a different server time zone than the other nodes on purpose, to check that
# `VALID FOR <interval> ON CLUSTER` stores the same absolute deadline regardless of node time zone.
ch4 = cluster.add_instance(
    "ch4",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/timezone.xml",
    ],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_access_control_on_cluster():
    ch1.query_with_retry(
        "CREATE USER IF NOT EXISTS Alex ON CLUSTER 'cluster'", retry_count=5
    )
    assert (
        ch2.query("SHOW CREATE USER Alex")
        == "CREATE USER Alex IDENTIFIED WITH no_password\n"
    )
    assert (
        ch1.query("SHOW CREATE USER Alex")
        == "CREATE USER Alex IDENTIFIED WITH no_password\n"
    )
    assert (
        ch3.query("SHOW CREATE USER Alex")
        == "CREATE USER Alex IDENTIFIED WITH no_password\n"
    )

    ch2.query_with_retry(
        "GRANT ON CLUSTER 'cluster' SELECT ON *.* TO Alex", retry_count=3
    )
    assert ch1.query("SHOW GRANTS FOR Alex") == "GRANT SELECT ON *.* TO Alex\n"
    assert ch2.query("SHOW GRANTS FOR Alex") == "GRANT SELECT ON *.* TO Alex\n"
    assert ch3.query("SHOW GRANTS FOR Alex") == "GRANT SELECT ON *.* TO Alex\n"

    ch3.query_with_retry(
        "REVOKE ON CLUSTER 'cluster' SELECT ON *.* FROM Alex", retry_count=3
    )
    assert ch1.query("SHOW GRANTS FOR Alex") == ""
    assert ch2.query("SHOW GRANTS FOR Alex") == ""
    assert ch3.query("SHOW GRANTS FOR Alex") == ""

    ch2.query_with_retry("DROP USER Alex ON CLUSTER 'cluster'", retry_count=3)
    assert "There is no user `Alex`" in ch1.query_and_get_error("SHOW CREATE USER Alex")
    assert "There is no user `Alex`" in ch2.query_and_get_error("SHOW CREATE USER Alex")
    assert "There is no user `Alex`" in ch3.query_and_get_error("SHOW CREATE USER Alex")


def test_grant_all_on_cluster():
    ch1.query("CREATE USER IF NOT EXISTS Alex ON CLUSTER 'cluster'")
    ch1.query("GRANT ALL ON *.* TO Alex ON CLUSTER 'cluster'")

    assert ch1.query("SHOW GRANTS FOR Alex") == "GRANT ALL ON *.* TO Alex\n"
    assert ch2.query("SHOW GRANTS FOR Alex") == "GRANT ALL ON *.* TO Alex\n"

    ch1.query("DROP USER Alex ON CLUSTER 'cluster'")


def test_grant_current_database_on_cluster():
    ch1.query("CREATE DATABASE user_db ON CLUSTER 'cluster'")
    ch1.query(
        "CREATE USER IF NOT EXISTS test_user ON CLUSTER 'cluster' DEFAULT DATABASE user_db"
    )
    ch1.query(
        "GRANT SELECT ON user_db.* TO test_user ON CLUSTER 'cluster' WITH GRANT OPTION"
    )
    ch1.query("GRANT CLUSTER ON *.* TO test_user ON CLUSTER 'cluster'")

    assert ch1.query("SHOW DATABASES", user="test_user") == "user_db\n"
    ch1.query("GRANT SELECT ON * TO test_user ON CLUSTER 'cluster'", user="test_user")
    assert ch1.query("SHOW DATABASES", user="test_user") == "user_db\n"
    ch1.query("DROP DATABASE user_db ON CLUSTER 'cluster'")
    ch1.query("DROP USER test_user ON CLUSTER 'cluster'")


def test_valid_for_on_cluster():
    # `VALID FOR <interval>` is a shortcut for `VALID UNTIL now + <interval>`. When distributed
    # `ON CLUSTER`, the interval must be resolved to an absolute deadline on the initiator, otherwise
    # every replica would re-evaluate `now + interval` against its own clock (and DDL queue latency),
    # so the stored `valid_until` would diverge across nodes. Here we assert that `SHOW CREATE USER`
    # is byte-identical on every replica, which only holds if the deadline was resolved exactly once.
    ch1.query("DROP USER IF EXISTS valid_for_user ON CLUSTER 'cluster'")

    # User-level `VALID FOR` together with a credential-level `VALID FOR`.
    ch1.query_with_retry(
        "CREATE USER valid_for_user ON CLUSTER 'cluster' "
        "IDENTIFIED WITH plaintext_password BY 'x' VALID FOR INTERVAL 1 YEAR",
        retry_count=5,
    )
    show = ch1.query("SHOW CREATE USER valid_for_user")
    # The shorthand must have been resolved to an absolute `VALID UNTIL` literal.
    assert "VALID UNTIL" in show
    assert "VALID FOR" not in show
    assert ch2.query("SHOW CREATE USER valid_for_user") == show
    assert ch3.query("SHOW CREATE USER valid_for_user") == show

    # `ALTER USER ... VALID FOR` must stay consistent across the cluster as well.
    ch2.query_with_retry(
        "ALTER USER valid_for_user ON CLUSTER 'cluster' VALID FOR INTERVAL 2 YEAR",
        retry_count=5,
    )
    altered = ch1.query("SHOW CREATE USER valid_for_user")
    assert "VALID UNTIL" in altered
    assert "VALID FOR" not in altered
    assert ch2.query("SHOW CREATE USER valid_for_user") == altered
    assert ch3.query("SHOW CREATE USER valid_for_user") == altered

    ch1.query("DROP USER valid_for_user ON CLUSTER 'cluster'")


def test_valid_for_on_cluster_multiple_auth_methods():
    # A user-level (global) `VALID FOR`/`VALID UNTIL` clause applies to every authentication method.
    # The query text distributed `ON CLUSTER` is formatted and re-parsed on every replica, and the
    # parser recognizes a global clause only before the `IDENTIFIED` list, so the formatter must print
    # the global clause first; otherwise the replicas would attach the deadline to the last method only.
    def stored_deadlines(node):
        # methods count, distinct deadlines count, and whether the first deadline is set at all
        return node.query(
            "SELECT length(valid_until), length(arrayDistinct(valid_until)), toUInt32(valid_until[1]) != 0 "
            "FROM system.users WHERE name = 'valid_for_multi_user'"
        ).strip()

    ch1.query("DROP USER IF EXISTS valid_for_multi_user ON CLUSTER 'cluster'")

    ch1.query_with_retry(
        "CREATE USER valid_for_multi_user ON CLUSTER 'cluster' "
        "VALID FOR INTERVAL 1 YEAR "
        "IDENTIFIED WITH plaintext_password BY 'a', plaintext_password BY 'b'",
        retry_count=5,
    )
    # Two methods, a single distinct non-zero deadline: the global deadline reached both methods.
    for node in (ch1, ch2, ch3):
        assert stored_deadlines(node) == "2\t1\t1"
    show = ch1.query("SHOW CREATE USER valid_for_multi_user")
    assert ch2.query("SHOW CREATE USER valid_for_multi_user") == show
    assert ch3.query("SHOW CREATE USER valid_for_multi_user") == show

    # The global deadline of an `ALTER ... ADD IDENTIFIED` must also apply to the methods that already
    # existed on the replica before the query.
    ch1.query_with_retry(
        "ALTER USER valid_for_multi_user ON CLUSTER 'cluster' "
        "VALID FOR INTERVAL 2 YEAR ADD IDENTIFIED WITH plaintext_password BY 'c'",
        retry_count=5,
    )
    for node in (ch1, ch2, ch3):
        assert stored_deadlines(node) == "3\t1\t1"

    ch1.query("DROP USER valid_for_multi_user ON CLUSTER 'cluster'")


def test_valid_for_on_cluster_mixed_timezone():
    # `cluster_tz` spans `ch1` and `ch4`, which run in different server time zones. The initiator
    # resolves `VALID FOR <interval>` to an absolute deadline and must serialize it with an explicit
    # time zone; otherwise every replica would re-interpret the bare wall-clock literal in its own
    # default time zone, so the stored epoch would diverge across the cluster. We compare the raw
    # `valid_until` epoch (which is time-zone independent, unlike the rendered `SHOW CREATE USER`).
    def stored_epoch(node):
        return node.query(
            "SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'valid_for_tz_user'"
        ).strip()

    ch1.query("DROP USER IF EXISTS valid_for_tz_user ON CLUSTER 'cluster_tz'")

    # Create from a node in the default time zone.
    ch1.query_with_retry(
        "CREATE USER valid_for_tz_user ON CLUSTER 'cluster_tz' "
        "IDENTIFIED WITH plaintext_password BY 'x' VALID FOR INTERVAL 1 YEAR",
        retry_count=5,
    )
    epoch_ch1 = stored_epoch(ch1)
    epoch_ch4 = stored_epoch(ch4)
    assert epoch_ch1 != "0"
    assert epoch_ch1 == epoch_ch4, (epoch_ch1, epoch_ch4)

    # Re-issue from the node in the other time zone; the deadline must still match on both nodes.
    ch4.query_with_retry(
        "ALTER USER valid_for_tz_user ON CLUSTER 'cluster_tz' VALID FOR INTERVAL 2 YEAR",
        retry_count=5,
    )
    altered_ch1 = stored_epoch(ch1)
    altered_ch4 = stored_epoch(ch4)
    assert altered_ch1 != epoch_ch1
    assert altered_ch1 == altered_ch4, (altered_ch1, altered_ch4)

    ch1.query("DROP USER valid_for_tz_user ON CLUSTER 'cluster_tz'")
