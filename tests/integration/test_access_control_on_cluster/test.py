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
