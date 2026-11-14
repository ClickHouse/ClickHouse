import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

# 26.4 supports lightweight updates and writes patch parts
# in v1 format (v2 format was introduced in 26.9).
OLD_VERSION = "26.4"

node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag=OLD_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

# A node with the latest version that runs with `compatibility` set
# to the old version in the default profile since startup.
node_compat = cluster.add_instance(
    "node_compat",
    user_configs=["configs/compatibility.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def check_patches_applied(node, comment, expected_sum, expected_modes):
    """
    Runs a query that applies patches on the fly and checks the result
    and the modes in which patch parts were applied.
    expected_modes is a tuple (Merge, Join, MergeOnKey) of 0/1 values.
    """
    result = node.query(
        f"SELECT sum(v) FROM t_lwu_compat SETTINGS log_comment = '{comment}'"
    )
    assert int(result) == expected_sum

    node.query("SYSTEM FLUSH LOGS")
    modes = node.query(f"""
        SELECT
            ProfileEvents['PatchesMergeAppliedInAllReadTasks'] > 0,
            ProfileEvents['PatchesJoinAppliedInAllReadTasks'] > 0,
            ProfileEvents['PatchesMergeOnKeyAppliedInAllReadTasks'] > 0
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select' AND log_comment = '{comment}'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """)
    assert tuple(int(x) for x in modes.split()) == expected_modes


def get_patch_partitions(node):
    """Returns a map of partition_id -> number of active parts for patch partitions."""
    result = node.query("""
        SELECT partition_id, count() FROM system.parts
        WHERE database = 'default' AND table = 't_lwu_compat'
            AND active AND startsWith(partition_id, 'patch-')
        GROUP BY partition_id
        """)
    partitions = {}
    for line in result.strip().splitlines():
        partition_id, count = line.split("\t")
        partitions[partition_id] = int(count)
    return partitions


def get_patch_columns(node):
    """Returns a sorted list of comma-separated column lists, one per active patch part."""
    result = node.query("""
        SELECT arrayStringConcat(arraySort(groupArray(column)), ',')
        FROM system.parts_columns
        WHERE database = 'default' AND table = 't_lwu_compat'
            AND active AND startsWith(partition_id, 'patch-')
        GROUP BY name
        """)
    return sorted(result.strip().splitlines())


PATCH_COLUMNS_V1 = "_block_number,_block_offset,_part,_part_data_version,_part_offset,v"
PATCH_COLUMNS_V2 = "_block_number,_block_offset,_part,_part_data_version,id,v"


def test_patch_parts_upgrade(started_cluster):
    node.query("DROP TABLE IF EXISTS t_lwu_compat SYNC")

    if not node.query("SELECT version()").startswith(OLD_VERSION):
        node.restart_with_original_version(clear_data_dir=True)

    # max_bytes_to_merge_at_max_space_in_pool = 1 disables background merges
    # (OPTIMIZE FINAL still works), so the set of parts is deterministic.
    node.query("""
        CREATE TABLE t_lwu_compat (id UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            apply_patches_on_merge = 0,
            max_bytes_to_merge_at_max_space_in_pool = 1
        """)

    node.query("INSERT INTO t_lwu_compat SELECT number, 0 FROM numbers(5000)")
    node.query("INSERT INTO t_lwu_compat SELECT number, 0 FROM numbers(5000, 5000)")

    # The patch has rows for both inserted parts and is applied in Merge mode.
    node.query("UPDATE t_lwu_compat SET v = v + 1 WHERE id < 6000")
    check_patches_applied(node, "old_version_merge_mode", 6000, (1, 0, 0))

    # After the source parts are merged away the patch is applied in Join mode.
    node.query("OPTIMIZE TABLE t_lwu_compat FINAL")
    check_patches_applied(node, "old_version_join_mode", 6000, (0, 1, 0))

    # The second patch is applied in Merge mode to the merged part,
    # so both modes are exercised in one query.
    node.query("UPDATE t_lwu_compat SET v = v + 1 WHERE id < 4000")
    check_patches_applied(node, "old_version_both_modes", 10000, (1, 1, 0))
    assert list(get_patch_partitions(node).values()) == [2]

    node.restart_with_latest_version()

    # Patches written by the old version are applied in the same modes after upgrade.
    check_patches_applied(node, "new_version_old_patches", 10000, (1, 1, 0))

    node.query("ALTER TABLE t_lwu_compat MODIFY SETTING patch_parts_version = 'v1'")
    node.query("UPDATE t_lwu_compat SET v = v + 1 WHERE id < 2000")
    check_patches_applied(node, "new_version_v1_patch", 12000, (1, 1, 0))

    node.query("ALTER TABLE t_lwu_compat MODIFY SETTING patch_parts_version = 'v2'")
    node.query("UPDATE t_lwu_compat SET v = v + 1 WHERE id < 1000")

    # All three modes are exercised in one query: Join for the patch created
    # before the merge, Merge for v1 patches, MergeOnKey for the v2 patch.
    check_patches_applied(node, "new_version_mixed_patches", 13000, (1, 1, 1))

    # Updates are applied on top of each other in the order of their creation.
    assert node.query(
        "SELECT id, v FROM t_lwu_compat WHERE id IN (0, 1500, 3500, 5500, 9000) ORDER BY id"
    ) == TSV([[0, 4], [1500, 3], [3500, 2], [5500, 1], [9000, 0]])

    # The structure hash used in patch partition ids changed, so v1 patches
    # written after the upgrade get a partition different from the partition
    # of v1 patches written by the old version, and the v2 patch gets its own
    # partition because its hash also includes column types and the sorting key.
    partitions = get_patch_partitions(node)
    assert sorted(partitions.values()) == [1, 1, 2]

    # Patch parts written by the old version can be merged on the new version.
    old_v1_partition = next(p for p, count in partitions.items() if count == 2)
    node.query(
        f"OPTIMIZE TABLE t_lwu_compat PARTITION ID '{old_v1_partition}' FINAL",
        settings={"optimize_throw_if_noop": 1},
    )
    assert get_patch_partitions(node)[old_v1_partition] == 1
    check_patches_applied(node, "new_version_merged_patches", 13000, (1, 1, 1))

    # Materialize mixed v1 and v2 patches on merge.
    node.query("ALTER TABLE t_lwu_compat MODIFY SETTING apply_patches_on_merge = 1")
    node.query("OPTIMIZE TABLE t_lwu_compat FINAL")

    assert (
        int(
            node.query("SELECT sum(v) FROM t_lwu_compat SETTINGS apply_patch_parts = 0")
        )
        == 13000
    )


def test_patch_parts_compatibility_setting(started_cluster):
    node_compat.query("DROP TABLE IF EXISTS t_lwu_compat SYNC")

    node_compat.query("""
        CREATE TABLE t_lwu_compat (id UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            apply_patches_on_merge = 0,
            max_bytes_to_merge_at_max_space_in_pool = 1
        """)

    # The default of `patch_parts_version` comes from `compatibility`,
    # it is not persisted in the table definition.
    assert "patch_parts_version" not in node_compat.query(
        "SHOW CREATE TABLE t_lwu_compat"
    )

    node_compat.query("INSERT INTO t_lwu_compat SELECT number, 0 FROM numbers(10000)")

    # The server runs with `compatibility` set to the old version in the
    # default profile, so the patch is written in v1 format: it contains
    # the `_part_offset` column and is applied in Merge mode.
    node_compat.query("UPDATE t_lwu_compat SET v = v + 1 WHERE id < 6000")
    check_patches_applied(node_compat, "compatibility_setting_v1", 6000, (1, 0, 0))
    assert get_patch_columns(node_compat) == [PATCH_COLUMNS_V1]

    # The explicit table setting takes precedence over `compatibility`.
    node_compat.query(
        "ALTER TABLE t_lwu_compat MODIFY SETTING patch_parts_version = 'v2'"
    )
    node_compat.query("UPDATE t_lwu_compat SET v = v + 1 WHERE id < 3000")
    check_patches_applied(node_compat, "compatibility_setting_v2", 9000, (1, 0, 1))

    # The v2 patch contains the sorting key column instead of `_part_offset`
    # and goes to its own partition.
    assert get_patch_columns(node_compat) == [PATCH_COLUMNS_V1, PATCH_COLUMNS_V2]
    assert sorted(get_patch_partitions(node_compat).values()) == [1, 1]
