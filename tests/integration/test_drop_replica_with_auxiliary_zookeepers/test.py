import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)


def create_aux_root(zk):
    # The zookeeper_aux auxiliary keeper is chrooted at /aux_root; the root must
    # exist before ClickHouse connects to it.
    zk.ensure_path("/aux_root")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_zookeeper_startup_command(create_aux_root)
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))


def test_drop_replica_in_auxiliary_zookeeper(started_cluster):
    drop_table([node1, node2], "test_auxiliary_zookeeper")
    for node in [node1, node2]:
        node.query("""
                CREATE TABLE test_auxiliary_zookeeper(a Int32)
                ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/test/test_auxiliary_zookeeper', '{replica}')
                ORDER BY a;
            """.format(replica=node.name))

    # stop node2 server
    node2.stop_clickhouse()
    time.sleep(5)

    # check is_active
    retries = 0
    max_retries = 5
    zk = cluster.get_kazoo_client("zoo1")
    while True:
        if (
            zk.exists(
                "/clickhouse/tables/test/test_auxiliary_zookeeper/replicas/node2/is_active"
            )
            is None
        ):
            break
        else:
            retries += 1
            if retries > max_retries:
                raise Exception("Failed to stop server.")
            time.sleep(1)

    # drop replica node2
    node1.query("SYSTEM DROP REPLICA 'node2'")

    assert zk.exists("/clickhouse/tables/test/test_auxiliary_zookeeper")
    assert (
        zk.exists("/clickhouse/tables/test/test_auxiliary_zookeeper/replicas/node2")
        is None
    )


def test_drop_replica_from_zkpath_in_auxiliary_zookeeper(started_cluster):
    # SYSTEM DROP REPLICA ... FROM ZKPATH 'aux:/path' must operate on the named
    # auxiliary keeper, not the default one. zookeeper_aux is chrooted at
    # /aux_root, so the table's znodes live under a namespace that does not exist
    # on the default keeper; routing the command to the wrong keeper would fail
    # to find the path.
    table_zk_path = "zookeeper_aux:/clickhouse/tables/test/test_from_zkpath_aux"
    inner_path = "/aux_root/clickhouse/tables/test/test_from_zkpath_aux"

    # The previous test leaves node2 stopped; make sure it is running here.
    node2.start_clickhouse()
    drop_table([node1, node2], "test_auxiliary_zookeeper")
    drop_table([node1, node2], "test_from_zkpath_aux")

    for node in [node1, node2]:
        node.query("""
                CREATE TABLE test_from_zkpath_aux(a Int32)
                ENGINE = ReplicatedMergeTree('{zk_path}', '{replica}')
                ORDER BY a;
            """.format(zk_path=table_zk_path, replica=node.name))

    # stop node2 server so its replica can be dropped remotely
    node2.stop_clickhouse()
    time.sleep(5)

    # wait until node2 is no longer active in the auxiliary keeper
    retries = 0
    max_retries = 5
    zk = cluster.get_kazoo_client("zoo1")
    while zk.exists(inner_path + "/replicas/node2/is_active") is not None:
        retries += 1
        if retries > max_retries:
            raise Exception("Failed to stop server.")
        time.sleep(1)

    # Targeting the default keeper (the pre-fix behaviour) would not find this
    # path, so a successful drop proves the command was routed to zookeeper_aux.
    node1.query(
        "SYSTEM DROP REPLICA 'node2' FROM ZKPATH '{zk_path}'".format(
            zk_path=table_zk_path
        )
    )

    assert zk.exists(inner_path)
    assert zk.exists(inner_path + "/replicas/node2") is None

    node2.start_clickhouse()
    drop_table([node1, node2], "test_from_zkpath_aux")


def test_drop_replica_from_zkpath_with_trailing_slashes(started_cluster):
    # A valid non-root ZKPATH with extra trailing slashes must be normalized so the
    # interpreter finds the "<path>/replicas" node. Before normalization was applied
    # in the parser, "<path>//" left a leftover slash and probed "<path>//replicas",
    # which does not exist, wrongly failing with "does not look like a table path".
    zk_path = "zookeeper_aux:/clickhouse/tables/test/test_zkpath_slashes"
    inner_path = "/aux_root/clickhouse/tables/test/test_zkpath_slashes"

    node2.start_clickhouse()
    drop_table([node1, node2], "test_zkpath_slashes")

    for node in [node1, node2]:
        node.query("""
                CREATE TABLE test_zkpath_slashes(a Int32)
                ENGINE = ReplicatedMergeTree('{zk_path}', '{replica}')
                ORDER BY a;
            """.format(zk_path=zk_path, replica=node.name))

    node2.stop_clickhouse()
    time.sleep(5)

    retries = 0
    max_retries = 5
    zk = cluster.get_kazoo_client("zoo1")
    while zk.exists(inner_path + "/replicas/node2/is_active") is not None:
        retries += 1
        if retries > max_retries:
            raise Exception("Failed to stop server.")
        time.sleep(1)

    # Trailing slashes must be collapsed; the drop must still find the table path.
    node1.query(
        "SYSTEM DROP REPLICA 'node2' FROM ZKPATH '{zk_path}//'".format(zk_path=zk_path)
    )

    assert zk.exists(inner_path)
    assert zk.exists(inner_path + "/replicas/node2") is None

    node2.start_clickhouse()
    drop_table([node1, node2], "test_zkpath_slashes")


def test_drop_replica_from_zkpath_not_blocked_by_default_keeper_table(started_cluster):
    # The self-protection guard must be keeper-aware: a local table on the DEFAULT
    # keeper that happens to share the same path string and replica name must not
    # block a drop that targets that path on an AUXILIARY keeper (a physically
    # different znode under /aux_root).
    #
    # node1 hosts a default-keeper decoy at the shared path with replica name
    # "shared_replica"; node2 hosts the auxiliary-keeper target at the same path
    # string with the same replica name. They live on different servers and
    # different keepers, so the interserver endpoints do not collide. Dropping
    # "shared_replica" via the auxiliary keeper from node1 hits node1's guard,
    # which used to match the decoy purely on the path string.
    shared_path = "/clickhouse/tables/test/test_shared_path"
    aux_zk_path = "zookeeper_aux:" + shared_path
    aux_inner_path = "/aux_root" + shared_path

    node2.start_clickhouse()
    drop_table([node1, node2], "test_shared_default")
    drop_table([node1, node2], "test_shared_aux")

    # Default-keeper decoy on node1 (stays alive throughout).
    node1.query("""
            CREATE TABLE test_shared_default(a Int32)
            ENGINE = ReplicatedMergeTree('{path}', 'shared_replica')
            ORDER BY a;
        """.format(path=shared_path))

    # Auxiliary-keeper target on node2, same path string and replica name.
    node2.query("""
            CREATE TABLE test_shared_aux(a Int32)
            ENGINE = ReplicatedMergeTree('{path}', 'shared_replica')
            ORDER BY a;
        """.format(path=aux_zk_path))

    # Stop node2 so its auxiliary-keeper replica can be dropped remotely.
    node2.stop_clickhouse()
    time.sleep(5)

    retries = 0
    max_retries = 5
    zk = cluster.get_kazoo_client("zoo1")
    while zk.exists(aux_inner_path + "/replicas/shared_replica/is_active") is not None:
        retries += 1
        if retries > max_retries:
            raise Exception("Failed to stop server.")
        time.sleep(1)

    # node1 still hosts the default-keeper decoy with the same path+replica string.
    # Before the guard became keeper-aware this drop was rejected with
    # TABLE_WAS_NOT_DROPPED ("There is a local table ..."); now it must succeed
    # against the auxiliary keeper and leave the default-keeper decoy untouched.
    node1.query(
        "SYSTEM DROP REPLICA 'shared_replica' FROM ZKPATH '{zk_path}'".format(
            zk_path=aux_zk_path
        )
    )

    # The auxiliary-keeper replica is gone.
    assert zk.exists(aux_inner_path + "/replicas/shared_replica") is None
    # The default-keeper decoy on node1 is untouched.
    assert zk.exists(shared_path + "/replicas/shared_replica") is not None

    node2.start_clickhouse()
    drop_table([node1, node2], "test_shared_default")
    drop_table([node1, node2], "test_shared_aux")


def test_drop_database_replica_from_zkpath_not_blocked_by_detached_default_keeper_db(
    started_cluster,
):
    # SYSTEM DROP DATABASE REPLICA ... FROM ZKPATH 'aux:/path' WITH TABLES scans local
    # detached database metadata to avoid dropping a replica that still exists locally.
    # That scan must be keeper-aware: a detached database on the DEFAULT keeper with the
    # same path string must not block a drop targeting that path on an AUXILIARY keeper.
    db_path = "/clickhouse/databases/test/detached_default_db"
    node1.query("DROP DATABASE IF EXISTS detached_default_db SYNC")

    node1.query(
        "CREATE DATABASE detached_default_db "
        "ENGINE = Replicated('{path}', 'shard1', 'r1')".format(path=db_path)
    )
    node1.query("DETACH DATABASE detached_default_db")

    # The detached database lives on the default keeper; targeting the same path on
    # zookeeper_aux is a different znode, so the guard must not report it. Before the
    # guard became keeper-aware the raw metadata path matched purely on the string and
    # this drop was wrongly rejected with "There is a detached database".
    err = node1.query_and_get_error(
        "SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 'shard1' "
        "FROM ZKPATH 'zookeeper_aux:{path}' WITH TABLES".format(path=db_path)
    )
    assert "There is a detached database" not in err

    node1.query("ATTACH DATABASE detached_default_db")
    node1.query("DROP DATABASE IF EXISTS detached_default_db SYNC")


def test_drop_database_replica_from_zkpath_blocked_by_detached_auxiliary_keeper_db(
    started_cluster,
):
    # The mirror case: a detached database that lives on the AUXILIARY keeper must be
    # recognized by the guard. Its metadata stores the raw engine argument with the
    # 'zookeeper_aux:' prefix, so before the guard normalized it the detached auxiliary
    # database never matched and the local detached replica could be dropped anyway.
    db_path = "/clickhouse/databases/test/detached_aux_db"
    node1.query("DROP DATABASE IF EXISTS detached_aux_db SYNC")

    node1.query(
        "CREATE DATABASE detached_aux_db "
        "ENGINE = Replicated('zookeeper_aux:{path}', 'shard1', 'r1')".format(
            path=db_path
        )
    )
    node1.query("DETACH DATABASE detached_aux_db")

    # Same keeper and same path as the detached database: the guard must protect it and
    # reject the drop with "There is a detached database".
    err = node1.query_and_get_error(
        "SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 'shard1' "
        "FROM ZKPATH 'zookeeper_aux:{path}' WITH TABLES".format(path=db_path)
    )
    assert "There is a detached database" in err

    node1.query("ATTACH DATABASE detached_aux_db")
    node1.query("DROP DATABASE IF EXISTS detached_aux_db SYNC")


def test_drop_database_replica_self_protection_with_trailing_slashes(started_cluster):
    # The self-protection guard for a LIVE local database must be robust to trailing slashes.
    # getZooKeeperPath() only strips a single trailing slash, so a database whose engine argument
    # has several ("/path///" keeps "/path/"), while the query path is fully collapsed to "/path".
    # Without canonicalizing both sides the guard compares "/path/" != "/path", returns early, and a
    # self-targeting SYSTEM DROP DATABASE REPLICA slips past the protection for the local replica.
    #
    # Such a database can only be produced from on-disk metadata (a plain CREATE collapses the path
    # via a second normalization), so we detach, rewrite the metadata with extra slashes, and reattach.
    db_path = "/clickhouse/databases/test/self_slash_db"
    node1.query("DROP DATABASE IF EXISTS self_slash_db SYNC")
    node1.query(
        "CREATE DATABASE self_slash_db "
        "ENGINE = Replicated('{path}', 'shard1', 'r1')".format(path=db_path)
    )
    node1.query("DETACH DATABASE self_slash_db")

    # The metadata is edited through clickhouse-disks so the test also works under the
    # "db disk" config, where database metadata lives on a remote disk rather than under
    # /var/lib/clickhouse/metadata. The database definition file is metadata/<db>.sql.
    metadata_path = "metadata/self_slash_db.sql"
    replace_text_in_metadata(
        node1,
        metadata_path,
        "Replicated('{path}'".format(path=db_path),
        "Replicated('{path}///'".format(path=db_path),
    )
    db_disk_name = get_database_disk_name(node1)
    if db_disk_name != "default":
        node1.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")
    node1.query("ATTACH DATABASE self_slash_db")

    # The live database now has getZooKeeperPath() == "/clickhouse/databases/test/self_slash_db/".
    # A self-targeting drop of its own replica (query path collapses to the same path without the
    # trailing slash) must be rejected as a local database, not silently allowed.
    err = node1.query_and_get_error(
        "SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 'shard1' "
        "FROM ZKPATH '{path}'".format(path=db_path)
    )
    assert "There is a local database" in err

    # This database was reattached from hand-edited metadata with an unusable keeper path, so a plain
    # DROP ... SYNC would hang trying to reach keeper; detach and remove its metadata directly instead.
    node1.query("DETACH DATABASE self_slash_db")
    disk_cmd_prefix = (
        f"/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml "
        f"--disk {db_disk_name} --save-logs --query "
    )
    node1.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'remove {metadata_path}'"]
    )


def test_drop_replica_self_protection_with_trailing_slashes(started_cluster):
    # The table-level self-protection guard must be robust to trailing slashes too.
    # getReplicaPath() is built from getZooKeeperPath(), which strips only a single trailing slash, so a
    # table whose engine argument has several ("/path///" keeps "/path//replicas/r"), while the query path
    # is fully collapsed to "/path/replicas/r". Without canonicalizing the table side the guard compares
    # "/path//replicas/r" != "/path/replicas/r", returns early, and a self-targeting SYSTEM DROP REPLICA
    # slips past the protection and destructively removes the local replica.
    #
    # Such a table can only be produced from on-disk metadata (a plain CREATE collapses the path via a
    # second normalization), so we detach, rewrite the metadata with extra slashes, and reattach.
    node2.start_clickhouse()
    drop_table([node1, node2], "self_slash_tbl")

    node1.query("""
            CREATE TABLE self_slash_tbl(a Int32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/self_slash_tbl', 'r1')
            ORDER BY a;
        """)
    node1.query("DETACH TABLE self_slash_tbl")

    # The metadata is edited through clickhouse-disks so the test also works under the
    # "db disk" config, where table metadata lives on a remote disk rather than under
    # /var/lib/clickhouse/metadata. metadata_path points at the table definition file.
    metadata_path = node1.query(
        "SELECT metadata_path FROM system.detached_tables "
        "WHERE database='default' AND table='self_slash_tbl'"
    ).strip()
    replace_text_in_metadata(
        node1, metadata_path, "test/self_slash_tbl'", "test/self_slash_tbl///'"
    )
    db_disk_name = get_database_disk_name(node1)
    if db_disk_name != "default":
        node1.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")
    node1.query("ATTACH TABLE self_slash_tbl")

    # The live table now has getReplicaPath() == "/clickhouse/tables/test/self_slash_tbl//replicas/r1".
    # A self-targeting drop of its own replica (query path collapses to the same path without the extra
    # slashes) must be rejected as a local table, not silently allowed to remove the local replica.
    err = node1.query_and_get_error(
        "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/test/self_slash_tbl///'"
    )
    assert "There is a local table" in err

    # The local replica must still be present in ZooKeeper (it was not destructively dropped).
    zk = cluster.get_kazoo_client("zoo1")
    assert zk.exists("/clickhouse/tables/test/self_slash_tbl/replicas/r1") is not None

    drop_table([node1, node2], "self_slash_tbl")
