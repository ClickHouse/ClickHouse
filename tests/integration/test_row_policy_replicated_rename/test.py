import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

# Node-local access storage: each node keeps its own copy of an access entity, so every replica
# re-keys its own policy while executing the rename. This is the configuration the fix serves.
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

# A replicated (shared) access storage in Keeper alongside a local one. An entity written here is
# a single global object, so re-keying it is visible to every server mounting the same path --
# including servers this rename does not apply to.
shared1 = cluster.add_instance(
    "shared1",
    main_configs=["configs/config.xml", "configs/replicated_access.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)
shared2 = cluster.add_instance(
    "shared2",
    main_configs=["configs/config.xml", "configs/replicated_access.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)
# Mounts the same access path as shared1/shared2 but owns an independent Atomic database, so it
# never sees their renames. It is the server a global re-key would silently unfilter.
shared3 = cluster.add_instance(
    "shared3",
    main_configs=["configs/config.xml", "configs/replicated_access.xml"],
    user_configs=["configs/readonly_policy.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 3},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

# Also on the shared access path, and allowed to create an Ordinary database so the startup
# conversion to Atomic -- a chain of renames -- runs through the same preflight.
convert_node = cluster.add_instance(
    "convert_node",
    main_configs=["configs/config.xml", "configs/replicated_access.xml"],
    user_configs=["configs/ordinary_settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 4},
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

nodes = [node1, node2]
shared_nodes = [shared1, shared2, shared3]
# Data is always 3 rows and the policy keeps `dept = 'eng'`, which matches exactly 1 of them.
# So a count of 1 means filtered and 3 means the policy no longer applies.
FILTERED = "1\n"
UNFILTERED = "3\n"
SKIP_WARNING = "Not moving"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _cleanup(instances, db):
    for n in instances:
        n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    # Access entities are node-local in the node1/node2 fixture, so drop them on each node. A policy
    # whose table was renamed is bound to the new name, so every candidate name must be covered.
    for n in instances:
        for table in ("ta", "tb", "ta_new"):
            for short_name in ("rp_a", "rp_b"):
                n.query(f"DROP ROW POLICY IF EXISTS {short_name} ON {db}.{table}")
        n.query("DROP USER IF EXISTS rp_user")


def _create_db(instances, db):
    for i, n in enumerate(instances, start=1):
        n.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/test/{db}', 'shard1', 'replica{i}')"
        )


def _sync(instances, db, tables):
    for n in instances:
        n.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
        for t in tables:
            n.query(f"SYSTEM SYNC REPLICA {db}.{t}")


def _make_atomic_table(instances, db, table="ta"):
    for n in instances:
        n.query(f"CREATE DATABASE IF NOT EXISTS {db} ENGINE = Atomic")
        n.query(
            f"CREATE TABLE {db}.{table} (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
        )
        n.query(f"INSERT INTO {db}.{table} VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")


def test_row_policy_follows_rename_in_replicated_database(started_cluster):
    """A user RENAME in a Replicated database travels through the DDL queue as SQL text and is
    re-executed independently by every replica, so each replica must re-key its own copy of the
    policy. Assert on EVERY replica that the policy is bound to the new name and that the
    restricted user still sees only its permitted row."""
    db = "rp_rename"
    _cleanup(nodes, db)
    _create_db(nodes, db)
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
    _sync(nodes, db, ["ta"])
    for n in nodes:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED

    try:
        node1.query(f"RENAME TABLE {db}.ta TO {db}.ta_new")
        _sync(nodes, db, ["ta_new"])

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
            assert n.query(f"SELECT count() FROM {db}.ta_new", user="rp_user") == FILTERED
            assert n.query(f"SELECT id FROM {db}.ta_new", user="rp_user") == "1\n"
    finally:
        _cleanup(nodes, db)


def test_row_policy_follows_exchange_in_replicated_database(started_cluster):
    """EXCHANGE TABLES in a Replicated database: both policies must cross with their data, on
    every replica. The two policies use different filters so a swap that dropped or mixed them
    up changes the restricted user's visible rows."""
    db = "rp_exchange"
    _cleanup(nodes, db)
    _create_db(nodes, db)
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
    _sync(nodes, db, ["ta", "tb"])
    for n in nodes:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == "1\n"
        assert n.query(f"SELECT count() FROM {db}.tb", user="rp_user") == "2\n"

    try:
        node1.query(f"EXCHANGE TABLES {db}.ta AND {db}.tb")
        _sync(nodes, db, ["ta", "tb"])

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
        _cleanup(nodes, db)


def test_no_rekey_with_shared_access_storage(started_cluster):
    """A replicated access storage holds ONE global policy object, while a RENAME applies only to
    the server that runs it. Re-keying would unbind the policy from the name the other servers
    still use, so it must not happen: shared2, which never renamed, keeps reading filtered.

    Both counts are asserted -- checking only shared2 would also pass if the rename had been
    rejected outright, which is a different (and wrong) behaviour."""
    db = "rp_shared"
    for n in shared_nodes[:2]:
        n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
        n.query("DROP USER IF EXISTS rp_user")
    _make_atomic_table(shared_nodes[:2], db)
    shared1.query("CREATE USER rp_user")
    shared1.query(f"GRANT SELECT ON {db}.* TO rp_user")
    shared1.query(
        f"CREATE ROW POLICY rp_a ON {db}.ta FOR SELECT USING dept = 'eng' TO rp_user"
    )
    # The policy really is in the shared storage and really is visible on both servers.
    assert (
        shared1.query("SELECT storage FROM system.row_policies WHERE short_name = 'rp_a'")
        == "replicated\n"
    )
    for n in shared_nodes[:2]:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED

    try:
        shared1.query(f"RENAME TABLE {db}.ta TO {db}.ta_new")
        # The renaming server sees the table unfiltered under its new name: the policy did not
        # follow. That is the accepted cost, and it is exactly what master does.
        assert shared1.query(f"SELECT count() FROM {db}.ta_new", user="rp_user") == UNFILTERED
        # The peer, which still has the old name, keeps its filter. Without the skip it read 3.
        assert shared2.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED
        assert (
            shared2.query(
                "SELECT database, table FROM system.row_policies WHERE short_name = 'rp_a'"
            )
            == f"{db}\tta\n"
        )
        assert shared1.contains_in_log(SKIP_WARNING)
    finally:
        for n in shared_nodes[:2]:
            n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
            n.query("DROP USER IF EXISTS rp_user")
        for name in ("ta", "ta_new"):
            shared1.query(f"DROP ROW POLICY IF EXISTS rp_a ON {db}.{name}")


def test_no_rekey_for_server_outside_the_renaming_group(started_cluster):
    """The set of servers sharing an access storage is not the set of servers sharing a rename.
    shared1 and shared2 form one Replicated database group; shared3 mounts the same access path
    but owns an independent Atomic database with a table of the same name. A global re-key driven
    by the group would unfilter shared3, which cannot be detected from inside the group -- hence
    the skip is keyed on the configuration rather than on who is renaming."""
    db = "rp_outside"
    for n in shared_nodes:
        n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
        n.query("DROP USER IF EXISTS rp_user")
    _create_db(shared_nodes[:2], db)
    shared1.query(
        f"CREATE TABLE {db}.ta (id UInt64, dept String) ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    shared1.query(f"INSERT INTO {db}.ta VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")
    # shared3 is not in the group: its own database happens to carry the same table name.
    _make_atomic_table([shared3], db)
    shared1.query("CREATE USER rp_user")
    shared1.query(f"GRANT SELECT ON {db}.* TO rp_user")
    shared1.query(
        f"CREATE ROW POLICY rp_a ON {db}.ta FOR SELECT USING dept = 'eng' TO rp_user"
    )
    _sync(shared_nodes[:2], db, ["ta"])
    for n in shared_nodes:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED

    try:
        shared1.query(f"RENAME TABLE {db}.ta TO {db}.ta_new")
        _sync(shared_nodes[:2], db, ["ta_new"])
        # The whole group renamed, so both group members read the new name unfiltered ...
        for n in shared_nodes[:2]:
            assert n.query(f"SELECT count() FROM {db}.ta_new", user="rp_user") == UNFILTERED
        # ... but shared3, outside the group, is untouched and still filtered. This is the case
        # every predicate computed from inside the group got wrong.
        assert shared3.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED
        assert (
            shared3.query(
                "SELECT database, table FROM system.row_policies WHERE short_name = 'rp_a'"
            )
            == f"{db}\tta\n"
        )
    finally:
        for n in shared_nodes:
            n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
            n.query("DROP USER IF EXISTS rp_user")
        for name in ("ta", "ta_new"):
            shared1.query(f"DROP ROW POLICY IF EXISTS rp_a ON {db}.{name}")


def test_no_rekey_on_rename_database_with_shared_access_storage(started_cluster):
    """RENAME DATABASE moves both `ON db.*` and `ON db.tbl` policies, through the same preflight.
    A shared storage must decline that too, so the peer's database keeps its policies."""
    db = "rp_sharedb"
    new_db = "rp_sharedb_new"
    for n in shared_nodes[:2]:
        for d in (db, new_db):
            n.query(f"DROP DATABASE IF EXISTS {d} SYNC")
        n.query("DROP USER IF EXISTS rp_user")
    _make_atomic_table(shared_nodes[:2], db)
    shared1.query("CREATE USER rp_user")
    shared1.query("GRANT SELECT ON *.* TO rp_user")
    shared1.query(
        f"CREATE ROW POLICY rp_a ON {db}.ta FOR SELECT USING dept = 'eng' TO rp_user"
    )
    for n in shared_nodes[:2]:
        assert n.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED

    try:
        shared1.query(f"RENAME DATABASE {db} TO {new_db}")
        assert shared1.query(f"SELECT count() FROM {new_db}.ta", user="rp_user") == UNFILTERED
        # The peer still has the old database name, and its policy is still bound to it.
        assert shared2.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED
        assert (
            shared2.query(
                "SELECT database, table FROM system.row_policies WHERE short_name = 'rp_a'"
            )
            == f"{db}\tta\n"
        )
        assert shared1.contains_in_log(SKIP_WARNING)
    finally:
        for n in shared_nodes[:2]:
            for d in (db, new_db):
                n.query(f"DROP DATABASE IF EXISTS {d} SYNC")
            n.query("DROP USER IF EXISTS rp_user")
        for d in (db, new_db):
            shared1.query(f"DROP ROW POLICY IF EXISTS rp_a ON {d}.ta")


def test_no_rekey_for_mixed_storages_leaves_both_names_filtered(started_cluster):
    """EXCHANGE with one policy in the shared storage and one in a node-local storage. Skipping
    only the shared policy would still move the local one onto the vacated name, leaving the
    other name with no policy at all -- and the collision check cannot see it, because the two
    policies have different short names. So the whole plan is dropped and both names keep their
    own policy, exactly as without the re-key."""
    db = "rp_mixed"
    for n in shared_nodes[:2]:
        n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
        n.query("DROP USER IF EXISTS rp_user")
    for n in shared_nodes[:2]:
        n.query(f"CREATE DATABASE {db} ENGINE = Atomic")
        for t in ("ta", "tb"):
            n.query(
                f"CREATE TABLE {db}.{t} (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
            )
        # ta: 1 'eng' + 2 'fin' + 3 'fin' -> rp_a keeps 'eng' -> 1 row
        # tb: 4 'ops' + 5 'fin' + 6 'fin' -> rp_b keeps 'ops' -> 1 row
        # Both populations are mixed, so each policy's filter is observable wherever it applies.
        n.query(f"INSERT INTO {db}.ta VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")
        n.query(f"INSERT INTO {db}.tb VALUES (4, 'ops'), (5, 'fin'), (6, 'fin')")
    shared1.query("CREATE USER rp_user")
    shared1.query(f"GRANT SELECT ON {db}.* TO rp_user")
    shared1.query(
        f"CREATE ROW POLICY rp_a ON {db}.ta IN replicated FOR SELECT USING dept = 'eng' TO rp_user"
    )
    shared1.query(
        f"CREATE ROW POLICY rp_b ON {db}.tb IN local_directory FOR SELECT USING dept = 'ops' TO rp_user"
    )
    # The fixture is only meaningful if the two policies really landed in different storages.
    assert (
        shared1.query(
            "SELECT short_name, storage FROM system.row_policies "
            "WHERE short_name IN ('rp_a', 'rp_b') ORDER BY short_name"
        )
        == "rp_a\treplicated\nrp_b\tlocal_directory\n"
    )
    # A policy created in the local storage right after one in the replicated storage is not
    # applied to new queries until the entity lists are reloaded. This reproduces identically on
    # master, so it is unrelated to this change; reload once so what follows measures the rename.
    shared1.query("SYSTEM RELOAD USERS")
    assert shared1.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED
    assert shared1.query(f"SELECT count() FROM {db}.tb", user="rp_user") == FILTERED

    try:
        shared1.query(f"EXCHANGE TABLES {db}.ta AND {db}.tb")
        # Neither policy moved, so each name still owns the policy it started with -- exactly the
        # bindings master has. That is the load-bearing part: a per-policy skip would keep the
        # shared rp_a on `ta` and still move the local rp_b from `tb` onto `ta`, leaving `tb` with
        # no policy at all and readable in full.
        assert (
            shared1.query(
                "SELECT short_name, database, table FROM system.row_policies "
                "WHERE short_name IN ('rp_a', 'rp_b') ORDER BY short_name"
            )
            == f"rp_a\t{db}\tta\nrp_b\t{db}\ttb\n"
        )
        # Each name is still filtered by the policy that guarded it, now applied to the data that
        # arrived: rp_a's 'eng' matches none of tb's rows and rp_b's 'ops' none of ta's. The point
        # is that neither name went unfiltered.
        assert shared1.query(f"SELECT count() FROM {db}.ta", user="rp_user") == "0\n"
        assert shared1.query(f"SELECT count() FROM {db}.tb", user="rp_user") == "0\n"
    finally:
        for n in shared_nodes[:2]:
            n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
            n.query("DROP USER IF EXISTS rp_user")
        for t in ("ta", "tb"):
            for short_name in ("rp_a", "rp_b"):
                shared1.query(f"DROP ROW POLICY IF EXISTS {short_name} ON {db}.{t}")


def test_no_rekey_for_mixed_storages_on_rename_onto_an_occupied_name(started_cluster):
    """The RENAME shape of the case above. Renaming `tb` onto `ta`'s name after `ta` is gone: the
    short names differ, so the destination-collision check does not fire and nothing but the
    all-or-nothing skip keeps `ta`'s shared policy in place while `tb`'s local one stays put."""
    db = "rp_mixed_rename"
    shared1.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    shared1.query("DROP USER IF EXISTS rp_user")
    shared1.query(f"CREATE DATABASE {db} ENGINE = Atomic")
    for t in ("ta", "tb"):
        shared1.query(
            f"CREATE TABLE {db}.{t} (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
        )
    shared1.query(f"INSERT INTO {db}.tb VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")
    shared1.query("CREATE USER rp_user")
    shared1.query(f"GRANT SELECT ON {db}.* TO rp_user")
    shared1.query(
        f"CREATE ROW POLICY rp_a ON {db}.ta IN replicated FOR SELECT USING dept = 'eng' TO rp_user"
    )
    shared1.query(
        f"CREATE ROW POLICY rp_b ON {db}.tb IN local_directory FOR SELECT USING dept = 'fin' TO rp_user"
    )
    assert (
        shared1.query(
            "SELECT short_name, storage FROM system.row_policies "
            "WHERE short_name IN ('rp_a', 'rp_b') ORDER BY short_name"
        )
        == "rp_a\treplicated\nrp_b\tlocal_directory\n"
    )
    # See the sibling test: a freshly created local-storage policy needs a reload before it applies.
    shared1.query("SYSTEM RELOAD USERS")
    assert shared1.query(f"SELECT count() FROM {db}.tb", user="rp_user") == "2\n"

    try:
        shared1.query(f"DROP TABLE {db}.ta SYNC")
        shared1.query(f"RENAME TABLE {db}.tb TO {db}.ta")
        # Both bindings unmoved. rp_a stayed on `ta`, so the arriving data is still filtered by it
        # ('eng' matches 1 of the 3 rows) instead of by the 'fin' policy that used to guard it.
        assert (
            shared1.query(
                "SELECT short_name, database, table FROM system.row_policies "
                "WHERE short_name IN ('rp_a', 'rp_b') ORDER BY short_name"
            )
            == f"rp_a\t{db}\tta\nrp_b\t{db}\ttb\n"
        )
        assert shared1.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED
    finally:
        shared1.query(f"DROP DATABASE IF EXISTS {db} SYNC")
        shared1.query("DROP USER IF EXISTS rp_user")
        for t in ("ta", "tb"):
            for short_name in ("rp_a", "rp_b"):
                shared1.query(f"DROP ROW POLICY IF EXISTS {short_name} ON {db}.{t}")


def test_readonly_policy_still_rejects_the_rename_on_a_shared_storage_server(started_cluster):
    """The read-only rejection must survive the skip. A policy from users.xml cannot be re-keyed at
    all, so its rename has to fail even on a server that also has a replicated storage -- where
    re-keys are otherwise declined silently. Evaluating the skip first would swallow the rejection
    and let the table be renamed out from under a filter that can never follow it."""
    db = "rp_readonly"
    shared3.query(f"DROP DATABASE IF EXISTS {db} SYNC")
    shared3.query(f"CREATE DATABASE {db} ENGINE = Atomic")
    shared3.query(
        f"CREATE TABLE {db}.rt (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
    )
    shared3.query(f"INSERT INTO {db}.rt VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")
    # The fixture is only meaningful if the policy is really read-only and the server really has a
    # replicated storage: those two together are what m4 is about.
    assert (
        shared3.query(
            f"SELECT storage FROM system.row_policies WHERE database = '{db}' AND table = 'rt'"
        )
        == "users_xml\n"
    )
    assert "replicated" in shared3.query("SELECT name FROM system.user_directories")
    assert shared3.query(f"SELECT count() FROM {db}.rt", user="ro_user") == FILTERED

    try:
        assert "ACCESS_STORAGE_READONLY" in shared3.query_and_get_error(
            f"RENAME TABLE {db}.rt TO {db}.rt_new"
        )
        # Nothing moved: the table kept its name and the filter still applies.
        assert (
            shared3.query(
                f"SELECT count() FROM system.tables WHERE database = '{db}' AND name = 'rt_new'"
            )
            == "0\n"
        )
        assert shared3.query(f"SELECT count() FROM {db}.rt", user="ro_user") == FILTERED
    finally:
        shared3.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_no_rekey_when_the_shared_policy_is_not_visible(started_cluster):
    """The skip must not depend on the shared policy being VISIBLE to the renaming server. A
    replicated storage answers reads from its own in-memory copy, refreshed from Keeper, so a
    policy written elsewhere while this server is cut off from Keeper is absent from the scan that
    collects the re-keys. Deciding from the affected entities would then find nothing to protect
    and move any node-local policy anyway; deciding from the configuration is unaffected.

    Keeper is partitioned rather than the server restarted: the storage refreshes its copy in full
    while constructing, so a restart WARMS it instead of leaving it cold."""
    db = "rp_cold"
    for n in shared_nodes[:2]:
        n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
        n.query("DROP USER IF EXISTS rp_user")
    _make_atomic_table(shared_nodes[:2], db)
    _make_atomic_table(shared_nodes[:2], db, "tb")
    shared2.query("CREATE USER rp_user")
    shared2.query(f"GRANT SELECT ON {db}.* TO rp_user")
    # A node-local policy on `tb`, which the skip must decline to move even though the shared
    # policy that motivates the skip is invisible below.
    shared1.query(
        f"CREATE ROW POLICY rp_b ON {db}.tb IN local_directory FOR SELECT USING dept = 'eng' TO rp_user"
    )

    try:
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(shared1)
            # Written on shared2 while shared1 cannot reach Keeper, so shared1 never sees it.
            shared2.query(
                f"CREATE ROW POLICY rp_a ON {db}.ta IN replicated FOR SELECT USING dept = 'eng' TO rp_user"
            )
            assert (
                shared1.query(
                    "SELECT count() FROM system.row_policies WHERE short_name = 'rp_a'"
                )
                == "0\n"
            ), "the fixture is vacuous: shared1 can see the shared policy after all"

            shared1.query(f"RENAME TABLE {db}.tb TO {db}.tb_new")
            # The node-local policy stayed on `tb`: the skip fired without consulting entities.
            assert (
                shared1.query(
                    "SELECT database, table FROM system.row_policies WHERE short_name = 'rp_b'"
                )
                == f"{db}\ttb\n"
            )
            assert shared1.contains_in_log(SKIP_WARNING)

        # Once Keeper is reachable again the shared policy is still on its original name, so
        # shared2 -- which never renamed -- is unaffected either way.
        assert (
            shared2.query(
                "SELECT database, table FROM system.row_policies WHERE short_name = 'rp_a'"
            )
            == f"{db}\tta\n"
        )
        assert shared2.query(f"SELECT count() FROM {db}.ta", user="rp_user") == FILTERED
    finally:
        for n in shared_nodes[:2]:
            n.query(f"DROP DATABASE IF EXISTS {db} SYNC")
            n.query("DROP USER IF EXISTS rp_user")
        for name in ("ta", "ta_new"):
            shared2.query(f"DROP ROW POLICY IF EXISTS rp_a ON {db}.{name}")
        for name in ("tb", "tb_new"):
            shared1.query(f"DROP ROW POLICY IF EXISTS rp_b ON {db}.{name}")


def test_startup_conversion_succeeds_with_a_shared_storage_policy(started_cluster):
    """The Ordinary -> Atomic startup conversion is a chain of renames, so it goes through the same
    preflight -- while the access storages have not finished reloading. Declining the re-key there
    must not throw: a throw would propagate out of the conversion and the server would refuse to
    start. Assert the server comes back up and the policy still filters."""
    db = "rp_convert"
    convert_node.query(f"DROP DATABASE IF EXISTS {db}")
    convert_node.query("DROP USER IF EXISTS rp_user")
    convert_node.query(f"CREATE DATABASE {db} ENGINE = Ordinary")
    convert_node.query(
        f"CREATE TABLE {db}.t (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
    )
    convert_node.query(f"INSERT INTO {db}.t VALUES (1, 'eng'), (2, 'fin'), (3, 'fin')")
    convert_node.query("CREATE USER rp_user")
    convert_node.query(f"GRANT SELECT ON {db}.* TO rp_user")
    convert_node.query(
        f"CREATE ROW POLICY rp_a ON {db}.t IN replicated FOR SELECT USING dept = 'eng' TO rp_user"
    )
    # A materialized view too: the conversion assigns a fresh UUID, so its inner table is renamed
    # from `.inner.<view>` to `.inner_id.<uuid>` and carries its own policy through the preflight.
    convert_node.query(
        f"CREATE MATERIALIZED VIEW {db}.mv ENGINE = MergeTree ORDER BY id "
        f"AS SELECT id, dept FROM {db}.t"
    )
    inner = convert_node.query(
        f"SELECT name FROM system.tables WHERE database = '{db}' AND name LIKE '.inner%'"
    ).strip()
    assert inner == ".inner.mv", inner
    convert_node.query(
        f"CREATE ROW POLICY rp_inner ON {db}.`{inner}` IN replicated "
        f"FOR SELECT USING dept = 'eng' TO rp_user"
    )
    assert convert_node.query(f"SELECT count() FROM {db}.t", user="rp_user") == FILTERED

    try:
        convert_node.stop_clickhouse()
        convert_node.exec_in_container(
            ["bash", "-c", "touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
        )
        # Fails here if the preflight throws during the conversion: the server does not come up.
        convert_node.start_clickhouse()

        assert (
            convert_node.query(f"SELECT engine FROM system.databases WHERE name = '{db}'")
            == "Atomic\n"
        )
        # The policies were not moved, and the conversion restores the original names, so the table
        # policy is still on the name the table ends up with and still filters.
        assert (
            convert_node.query(
                "SELECT database, table FROM system.row_policies WHERE short_name = 'rp_a'"
            )
            == f"{db}\tt\n"
        )
        assert convert_node.query(f"SELECT count() FROM {db}.t", user="rp_user") == FILTERED
    finally:
        convert_node.exec_in_container(
            ["bash", "-c", "rm -f /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
        )
        convert_node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
        convert_node.query("DROP USER IF EXISTS rp_user")
        convert_node.query(f"DROP ROW POLICY IF EXISTS rp_a ON {db}.t")
        convert_node.query(f"DROP ROW POLICY IF EXISTS rp_inner ON {db}.`{inner}`")
