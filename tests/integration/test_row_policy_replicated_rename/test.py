import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _cleanup(db):
    for n in nodes:
        n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    # Access entities are node-local in this fixture, so drop them on each node. A policy whose
    # table was renamed is bound to the new name, so every candidate name must be covered.
    for n in nodes:
        for table in ("ta", "tb", "ta_new"):
            for short_name in ("rp_a", "rp_b"):
                n.query(f"DROP ROW POLICY IF EXISTS {short_name} ON {db}.{table}")
        n.query("DROP USER IF EXISTS rp_user")


def _create_db(db):
    for i, n in enumerate(nodes, start=1):
        n.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/test/{db}', 'shard1', 'replica{i}')"
        )


def _sync(db, tables):
    for n in nodes:
        n.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
        for t in tables:
            n.query(f"SYSTEM SYNC REPLICA {db}.{t}")


def test_row_policy_follows_rename_in_replicated_database(started_cluster):
    """A user RENAME in a Replicated database travels through the DDL queue as SQL text and is
    re-executed independently by every replica, so each replica must re-key its own copy of the
    policy. Assert on EVERY replica that the policy is bound to the new name and that the
    restricted user still sees only its permitted row."""
    db = "rp_rename"
    _cleanup(db)
    _create_db(db)
    node1.query(
        f"CREATE TABLE {db}.ta (id UInt64, dept String) ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    node1.query(f"INSERT INTO {db}.ta VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")
    for n in nodes:
        n.query("CREATE USER rp_user")
        n.query(f"GRANT SELECT ON {db}.* TO rp_user")
        n.query(
            f"CREATE ROW POLICY rp_a ON {db}.ta FOR SELECT USING dept = 'eng' TO rp_user"
        )
    _sync(db, ["ta"])
    for n in nodes:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == "1\n"

    try:
        node1.query(f"RENAME TABLE {db}.ta TO {db}.ta_new")
        _sync(db, ["ta_new"])

        for n in nodes:
            # The policy followed the table: exact binding on this replica.
            assert (
                n.query(
                    f"SELECT database, table FROM system.row_policies "
                    f"WHERE short_name = 'rp_a' AND database = '{db}'"
                )
                == f"{db}\tta_new\n"
            )
            # ... and it actually filters under the new name. The true row count is 3.
            assert (
                n.query(
                    f"SELECT sum(rows) FROM system.parts "
                    f"WHERE database = '{db}' AND table = 'ta_new' AND active"
                )
                == "3\n"
            )
            assert n.query(f"SELECT count() FROM {db}.ta_new", user="rp_user") == "1\n"
            assert n.query(f"SELECT id FROM {db}.ta_new", user="rp_user") == "1\n"
    finally:
        _cleanup(db)


def test_row_policy_follows_exchange_in_replicated_database(started_cluster):
    """EXCHANGE TABLES in a Replicated database: both policies must cross with their data, on
    every replica. The two policies use different filters so a swap that dropped or mixed them
    up changes the restricted user's visible rows."""
    db = "rp_exchange"
    _cleanup(db)
    _create_db(db)
    node1.query(
        f"CREATE TABLE {db}.ta (id UInt64, dept String) ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    node1.query(
        f"CREATE TABLE {db}.tb (id UInt64, dept String) ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    # ta: 1 'eng' + 2 'fin'  -> policy rp_a keeps 'eng'  -> 1 row
    # tb: 3 'ops' + 4 'ops'  -> policy rp_b keeps 'ops'  -> 2 rows
    node1.query(f"INSERT INTO {db}.ta VALUES (1, 'eng'), (2, 'fin')")
    node1.query(f"INSERT INTO {db}.tb VALUES (3, 'ops'), (4, 'ops')")
    for n in nodes:
        n.query("CREATE USER rp_user")
        n.query(f"GRANT SELECT ON {db}.* TO rp_user")
        n.query(
            f"CREATE ROW POLICY rp_a ON {db}.ta FOR SELECT USING dept = 'eng' TO rp_user"
        )
        n.query(
            f"CREATE ROW POLICY rp_b ON {db}.tb FOR SELECT USING dept = 'ops' TO rp_user"
        )
    _sync(db, ["ta", "tb"])
    for n in nodes:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == "1\n"
        assert n.query(f"SELECT count() FROM {db}.tb", user="rp_user") == "2\n"

    try:
        node1.query(f"EXCHANGE TABLES {db}.ta AND {db}.tb")
        _sync(db, ["ta", "tb"])

        for n in nodes:
            # Each policy followed its own data across the swap.
            assert (
                n.query(
                    f"SELECT table FROM system.row_policies "
                    f"WHERE short_name = 'rp_a' AND database = '{db}'"
                )
                == "tb\n"
            )
            assert (
                n.query(
                    f"SELECT table FROM system.row_policies "
                    f"WHERE short_name = 'rp_b' AND database = '{db}'"
                )
                == "ta\n"
            )
            # The name `ta` now holds tb's old data (3 'ops', 4 'ops'), filtered by rp_b.
            assert (
                n.query(f"SELECT id FROM {db}.ta ORDER BY id", user="rp_user")
                == "3\n4\n"
            )
            # The name `tb` now holds ta's old data (1 'eng', 2 'fin'), filtered by rp_a.
            assert n.query(f"SELECT id FROM {db}.tb", user="rp_user") == "1\n"
    finally:
        _cleanup(db)
