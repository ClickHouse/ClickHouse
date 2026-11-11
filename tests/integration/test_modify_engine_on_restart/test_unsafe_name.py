import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import get_table_path, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters_name_path.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_unsafe_name"

# A name-based `default_replica_path` splices the table's own name into the Keeper path, so a name
# carrying '/' resolves inside another table's subtree. `victim` is the co-tenant that gets damaged;
# the ghost's name is exactly the replica path underneath it.
VICTIM = "victim"
GHOST = "victim/replicas/ghost"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return ch1.query(database=database_name, sql=query)


def victim_total_replicas():
    return q(
        f"SELECT total_replicas FROM system.replicas WHERE database = '{database_name}' AND table = '{VICTIM}'"
    ).strip()


def active_part_paths(table):
    """Absolute in-container paths of the table's active parts, each with a trailing '/'.

    `system.parts` rather than a directory listing: a MergeTree table's data directory always holds
    a top-level `detached` directory too, and Outdated parts keep their own, so enumerating
    directories counts more than the parts.
    """
    return q(
        "SELECT path FROM system.parts"
        f" WHERE database = '{database_name}' AND table = '{table}' AND active"
    ).split()


def count_txn_version_files(table):
    path = get_table_path(ch1, table, database_name)
    return int(
        ch1.exec_in_container(
            ["bash", "-c", f"find {path} -name txn_version.txt | wc -l"]
        ).strip()
    )


def plant_txn_version_files(table):
    """Write a valid non-transactional `txn_version.txt` onto every active part of `table`.

    Transactions are off by default, so parts carry no such file and a bare count of them cannot
    tell whether the conversion removed anything. The content is the form `VersionInfo` itself
    emits, so a surviving file still loads.
    """
    nil = "00000000-0000-0000-0000-000000000000"
    content = (
        "version: 1\\n"
        "storing_version: 1\\n"
        f"creation_tid: (1, 1, {nil})\\n"
        "creation_csn: 1\\n"
        f"removal_tid: (0, 0, {nil})\\n"
        "removal_csn: 0"
    )
    for part_path in active_part_paths(table):
        ch1.exec_in_container(
            ["bash", "-c", f'printf "{content}" > "{part_path}txn_version.txt"']
        )


def test_attach_as_replicated_rejects_unsafe_name(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q(f"CREATE TABLE `{VICTIM}` ( A Int64 ) ENGINE = ReplicatedMergeTree ORDER BY A")
    q(f"INSERT INTO `{VICTIM}` VALUES (1)")
    # The whole scenario needs the path to be derived from the name, so assert that rather than
    # assuming the config took effect.
    assert (
        q(
            f"SELECT zookeeper_path FROM system.replicas WHERE database = '{database_name}' AND table = '{VICTIM}'"
        ).strip()
        == f"/clickhouse/tables/{database_name}/{VICTIM}"
    )
    assert victim_total_replicas() == "1"

    # Merges are pinned off so the set of active parts, and therefore the planted file count, is the
    # same before the conversion and after the re-attach below.
    q(
        f"CREATE TABLE `{GHOST}` ( A Int64 ) ENGINE = MergeTree ORDER BY A"
        " SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1"
    )
    q(f"INSERT INTO `{GHOST}` VALUES (7)")
    q(f"INSERT INTO `{GHOST}` VALUES (8)")
    planted = len(active_part_paths(GHOST))
    # Arming assertion: with no part to plant onto, the file count reads 0 in every arm and the
    # transaction-metadata assertion below discriminates nothing.
    assert planted > 0
    plant_txn_version_files(GHOST)
    assert count_txn_version_files(GHOST) == planted
    q(f"DETACH TABLE `{GHOST}`")

    # The conversion must be refused. Without the check it succeeds and the table takes a path
    # under the victim's own subtree.
    assert "BAD_ARGUMENTS" in ch1.query_and_get_error(
        f"ATTACH TABLE `{GHOST}` AS REPLICATED", database=database_name
    )

    # The rejection ran before the metadata rewrite, so a plain ATTACH brings the table back as
    # the MergeTree it always was. A rejection sited after the rewrite reports ReplicatedMergeTree.
    q(f"ATTACH TABLE `{GHOST}`")
    assert (
        q(
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = '{GHOST}'"
        ).strip()
        == "MergeTree"
    )

    # It also ran before the table's transaction metadata was removed. That removal is
    # irreversible, so the file count is what pins the check ahead of `clearTransactionMetadata`; the
    # row count below is a plain no-regression line.
    assert count_txn_version_files(GHOST) == planted
    assert q(f"SELECT count() FROM `{GHOST}`").strip() == "2"

    # The victim is untouched, and still accepts a metadata ALTER. A planted ghost replica
    # leaves this failing with a Keeper error over the ghost's missing log_pointer.
    assert victim_total_replicas() == "1"
    assert (
        q(
            f"SELECT groupArray(name) FROM system.zookeeper WHERE path = '/clickhouse/tables/{database_name}/{VICTIM}/replicas'"
        ).strip()
        == "['node1']"
    )
    q(f"ALTER TABLE `{VICTIM}` ADD COLUMN B UInt64 SETTINGS alter_sync = 2")

    # Control: a path-safe name converts fine under the very same configuration, so the
    # rejection above is about the name and not about the config or the conversion route.
    q("CREATE TABLE safe_name ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q("INSERT INTO safe_name VALUES (1), (2)")
    q("DETACH TABLE safe_name")
    q("ATTACH TABLE safe_name AS REPLICATED")
    assert (
        q(
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = 'safe_name'"
        ).strip()
        == "ReplicatedMergeTree"
    )
    assert q("SELECT count() FROM safe_name").strip() == "2"

    # Control: the reverse direction mints no Keeper path, so an unsafe name must not block it.
    # The table has to be replicated already AND unsafely named, which conversion cannot produce, so
    # it is created directly with an explicit path outside the victim's subtree.
    q(
        f"CREATE TABLE `{GHOST}2` ( A Int64 )"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/unrelated/{database_name}', 'node1') ORDER BY A"
    )
    q(f"INSERT INTO `{GHOST}2` VALUES (1), (2)")
    q(f"DETACH TABLE `{GHOST}2`")
    q(f"ATTACH TABLE `{GHOST}2` AS NOT REPLICATED")
    assert (
        q(
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = '{GHOST}2'"
        ).strip()
        == "MergeTree"
    )
    assert q(f"SELECT count() FROM `{GHOST}2`").strip() == "2"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_convert_flag_rejects_unsafe_name(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q(f"CREATE TABLE `{GHOST}` ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q(f"INSERT INTO `{GHOST}` VALUES (1), (2)")
    set_convert_flags(ch1, database_name, [GHOST])
    # Read while the server is up: every helper here answers from a query, and between the stop and
    # the recovery start below there is provably no server to answer one.
    table_data_path = get_table_path(ch1, GHOST, database_name)

    # The flag-file route refuses the same conversion, and because it runs during startup the
    # server does not come up. That is the behaviour the sibling `checkReplicaPathExists` already
    # has on this route (see test_zk_path_exists.py), and the recovery is the same: delete the flag.
    ch1.stop_clickhouse()
    ch1.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    ch1.exec_in_container(
        ["bash", "-c", f"rm {table_data_path}convert_to_replicated"]
    )
    ch1.start_clickhouse()

    # The table is intact: still a MergeTree, still holding its rows.
    assert (
        q(
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = '{GHOST}'"
        ).strip()
        == "MergeTree"
    )
    assert q(f"SELECT count() FROM `{GHOST}`").strip() == "2"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
