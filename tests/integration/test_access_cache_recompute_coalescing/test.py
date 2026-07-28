import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

NUM_POLICIES = 50
NUM_ROLES = 50


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_row_policy_recalculations(node):
    value = node.query(
        "SELECT value FROM system.events WHERE event = 'RowPolicyCacheRecalculations'"
    ).strip()
    return int(value) if value else 0


# A refresh that touches many entities at once (here: a ZooKeeper session loss that makes node2
# reread everything) delivers one notification per entity. The row policy cache must coalesce the
# expensive re-mix to once per batch instead of running it once per changed policy.
def test_recompute_coalesced_on_reread(started_cluster):
    node1.query(
        "CREATE TABLE IF NOT EXISTS default.t (x Int32) ENGINE = Memory;"
        "CREATE USER u IDENTIFIED WITH no_password;"
        + "".join(
            f"CREATE ROW POLICY p{i} ON default.t USING x = {i} TO u;"
            for i in range(NUM_POLICIES)
        )
    )
    node2.query("CREATE TABLE IF NOT EXISTS default.t (x Int32) ENGINE = Memory")

    node2.query_with_retry(
        "SELECT count() FROM system.row_policies WHERE short_name LIKE 'p%'",
        check_callback=lambda r: r.strip() == str(NUM_POLICIES),
    )

    # Build a live EnabledRowPolicies set for u on node2 (this also subscribes RowPolicyCache).
    node2.query("SELECT 1", user="u")

    baseline = get_row_policy_recalculations(node2)

    # All three steps are required and none can be dropped:
    #  - PartitionManager makes node2 *miss* the changes, so they are not consumed one-by-one as
    #    per-entity watches (each of which would be its own notification batch the coalescing can't
    #    help with);
    #  - SYSTEM RECONNECT ZOOKEEPER drops node2's session so the reconnect rereads everything in one
    #    `all=true` pass (a surviving session would redeliver the missed changes as NUM_POLICIES
    #    separate watch notifications);
    #  - SYSTEM RELOAD USERS is the deterministic flush (reload + synchronous sendNotifications).
    # SYSTEM RELOAD USERS on its own is not enough: on an up-to-date node `setAll` diffs to zero and
    # emits nothing.
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node2)
        node1.query(
            "".join(
                f"ALTER ROW POLICY p{i} ON default.t USING x = {i} + 1000;"
                for i in range(NUM_POLICIES)
            )
        )
        node2.query("SYSTEM RECONNECT ZOOKEEPER")

    node2.query_with_retry("SYSTEM RELOAD USERS")

    delta = get_row_policy_recalculations(node2) - baseline

    node1.query(
        "DROP USER u;"
        + "".join(f"DROP ROW POLICY p{i} ON default.t;" for i in range(NUM_POLICIES))
        + "DROP TABLE default.t SYNC;"
    )
    node2.query("DROP TABLE default.t SYNC")

    # Lower bound guards against a silently broken setup (cache not subscribed / batch never fired):
    # the reread must recompute at least once. Upper bound is the coalescing guarantee: once per
    # batch (allow 2 for the reconnect/reload reread overlapping), not once per changed policy (~50).
    assert 1 <= delta <= 2, (
        f"row policy cache recomputed {delta} times for a {NUM_POLICIES}-entity reread; "
        f"expected 1 (coalesced); without coalescing it would be ~{NUM_POLICIES}"
    )


def get_role_recalculations(node):
    value = node.query(
        "SELECT value FROM system.events WHERE event = 'RoleCacheRecalculations'"
    ).strip()
    return int(value) if value else 0


# The same multi-entity reread, but for the role cache: a user with many granted roles, every role
# altered at once while node2 misses the changes, then a single reread. RoleCache subscribes for all
# roles in one batched subscription and must coalesce collectEnabledRoles to once per batch instead of
# once per changed role.
def test_role_recompute_coalesced_on_reread(started_cluster):
    node1.query(
        "CREATE USER u2 IDENTIFIED WITH no_password;"
        + "".join(f"CREATE ROLE r{i};" for i in range(NUM_ROLES))
        + "".join(f"GRANT r{i} TO u2;" for i in range(NUM_ROLES))
        + "ALTER USER u2 DEFAULT ROLE ALL;"
    )

    node2.query_with_retry(
        "SELECT count() FROM system.roles WHERE name LIKE 'r%'",
        check_callback=lambda r: r.strip() == str(NUM_ROLES),
    )

    # Build a live EnabledRoles set for u2 on node2: this subscribes RoleCache and marks every granted
    # role as referenced, so a later change to one of them triggers a recalculation.
    node2.query_with_retry("SELECT 1", user="u2")

    baseline = get_role_recalculations(node2)

    # Same three required steps as in test_recompute_coalesced_on_reread (see the comment there).
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node2)
        node1.query(
            "".join(
                f"ALTER ROLE r{i} SETTINGS max_threads = {i + 1};"
                for i in range(NUM_ROLES)
            )
        )
        node2.query("SYSTEM RECONNECT ZOOKEEPER")

    node2.query_with_retry("SYSTEM RELOAD USERS")

    delta = get_role_recalculations(node2) - baseline

    node1.query(
        "DROP USER u2;" + "".join(f"DROP ROLE r{i};" for i in range(NUM_ROLES))
    )

    # Same bounds as the row policy case: at least one recompute (guards a broken setup), at most two
    # (coalesced per batch), not once per changed role (~50).
    assert 1 <= delta <= 2, (
        f"role cache recomputed {delta} times for a {NUM_ROLES}-role reread; "
        f"expected 1 (coalesced); without coalescing it would be ~{NUM_ROLES}"
    )
