import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from ast import literal_eval
import logging
from contextlib import contextmanager
import time

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    use_keeper=False,
    main_configs=["configs/metadata_version.xml"],
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2", with_zookeeper=True, use_keeper=False, stay_alive=True
)

all_nodes = [node1, node2]

CONFIG = """
<clickhouse>
    <merge_tree>
        <desired_part_metadata_format_version>{}</desired_part_metadata_format_version>
    </merge_tree>
</clickhouse>
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_desired_metadata_format_version():
    # Basic verification that our new setting is being loaded as expected
    # On node1 we explicitly enabled the feature by setting the value to 1.
    setting = node1.query(
        "SELECT value,changed FROM system.merge_tree_settings WHERE name='desired_part_metadata_format_version'"
    )
    assert setting.strip() == "1\t1"

    # Node2 has the default value for the setting which should be zero, since it's still experimental
    setting = node2.query(
        "SELECT value,changed FROM system.merge_tree_settings WHERE name='desired_part_metadata_format_version'"
    )
    assert setting.strip() == "0\t0"


def test_plain_merge_tree():
    # Verify that for a non-replicated mergetree table, the parts have creation time recorded correctly according to the setting.
    for node in all_nodes:
        node.query(
            """
            CREATE TABLE test_plain_merge_tree(ts DateTime, id UInt32)
            ENGINE=MergeTree
            PARTITION BY toYYYYMMDD(ts)
            ORDER BY ts
            """
        )
        node.query("INSERT INTO test_plain_merge_tree VALUES (now(), 0)")

    # node1 enables our new metadata, so we expect parts to have creation time populated
    res = node1.query(
        "SELECT creation_time BETWEEN now()-1 AND now(), metadata_format_version FROM system.parts WHERE active and table='test_plain_merge_tree'"
    )
    assert res == "1\t1\n"

    # node2 disables the new metadata, so we should see no creation time
    res = node2.query(
        "SELECT creation_time, metadata_format_version FROM system.parts WHERE active and table='test_plain_merge_tree'"
    )
    assert res == "\\N\t0\n"

    # On node1
    # If we run some no-op mutation, the modification time will change but the creation time will stay the same
    # Start by getting original creation time:
    original_create_time = node1.query(
        "SELECT creation_time FROM system.parts WHERE active and table='test_plain_merge_tree'"
    )
    time.sleep(3)
    node1.query(
        "ALTER TABLE test_plain_merge_tree UPDATE id=id+1 WHERE false SETTINGS mutations_sync=2"
    )
    res = node1.query(
        "SELECT modification_time-2 > creation_time FROM system.parts WHERE active and table='test_plain_merge_tree'"
    )
    assert res == "1\n"
    # Also verify that the create time is identical, since the part hasn't been changed other than the name
    altered_create_time = node1.query(
        "SELECT creation_time FROM system.parts WHERE active and table='test_plain_merge_tree'"
    )
    assert original_create_time == altered_create_time


def test_replicated_part_metadata():
    # Now we will test that replicated merge trees create the new metadata as designed
    for i, node in enumerate(all_nodes):
        node.query(
            f"""
            CREATE TABLE test_replicated_part_metadata(ts DateTime, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_replicated_part_metadata', '{i+1}')
            PARTITION BY toYYYYMMDD(ts) ORDER BY ts
            """
        )
        # to avoid any extra merging happening until we want it:
        node.query("SYSTEM STOP MERGES")

    zk = cluster.get_kazoo_client("zoo1")

    # verify that the setting only is written to zookeeper if it has a value greater than zero
    # i.e. the new feature is enabled
    res, _ = zk.get("/clickhouse/tables/test_replicated_part_metadata/replicas/1/host")
    assert b"\ndesired_part_metadata_format_version: 1\n" in res
    # node2 has the setting at zero, so it's address info shouldn't include this line:
    res, _ = zk.get("/clickhouse/tables/test_replicated_part_metadata/replicas/2/host")
    assert b"\ndesired_part_metadata_format_version:" not in res

    # We can assert that both nodes observe the same desired version for the table:
    for node in all_nodes:
        assert "0\n" == node.query(
            "SELECT part_metadata_format_version FROM system.tables where name='test_replicated_part_metadata'"
        )

    # Insert some data so we can see part is created without new metadata
    node1.query(
        "INSERT INTO test_replicated_part_metadata SETTINGS insert_quorum='auto' VALUES (now()-2, 0, 123) (now()-1, 1, 234)"
    )
    for node in all_nodes:
        assert "1\t0\n" == node.query(
            "SELECT isNull(creation_time), metadata_format_version FROM system.parts WHERE active and table='test_replicated_part_metadata'"
        )

    # Let's bump node2 to desire the newer node format version
    node2.replace_config(
        "/etc/clickhouse-server/config.d/metadata_version.xml", CONFIG.format(1)
    )
    node2.restart_clickhouse()

    # Now both nodes agree to use the new metadata format version, newly written part should use it:
    node1.query(
        "INSERT INTO test_replicated_part_metadata VALUES (now()-2, 0, 345) (now()-1, 1, 456)"
    )

    # We now expect both nodes to agree about the setting, and about desired version for new parts
    # We can assert that both nodes observe the same desired version for the table:
    for i, node in enumerate(all_nodes):
        assert "1\t1\n" == node.query(
            "SELECT value,changed FROM system.merge_tree_settings WHERE name='desired_part_metadata_format_version'"
        )
        assert "1\n" == node.query(
            "SELECT part_metadata_format_version FROM system.tables where name='test_replicated_part_metadata'"
        )
        res, _ = zk.get(
            f"/clickhouse/tables/test_replicated_part_metadata/replicas/{i+1}/host"
        )
        assert b"\ndesired_part_metadata_format_version: 1\n" in res
        # Old part should still not have creation time, new part should have creation time
        assert "1\t0\n0\t1\n" == node.query(
            "SELECT isNull(creation_time), metadata_format_version FROM system.parts WHERE active and table='test_replicated_part_metadata' ORDER BY modification_time"
        )
        node.query("SYSTEM START MERGES")

    # Verify that if we optimize, both parts combine into a single part with the new metadata
    node1.query("OPTIMIZE TABLE test_replicated_part_metadata FINAL")
    for i, node in enumerate(all_nodes):
        assert "0\t1\n" == node.query(
            "SELECT isNull(creation_time), metadata_format_version FROM system.parts WHERE active and table='test_replicated_part_metadata'"
        )

    # If we run some no-op mutation, the modification time will change but the creation time will stay the same
    original_create_time = node1.query(
        "SELECT creation_time FROM system.parts WHERE active and table='test_replicated_part_metadata'"
    )
    time.sleep(2)
    node1.query(
        "ALTER TABLE test_replicated_part_metadata UPDATE id=id+1 WHERE false SETTINGS mutations_sync=2"
    )
    for i, node in enumerate(all_nodes):
        assert original_create_time == node.query(
            "SELECT creation_time FROM system.parts WHERE active and table='test_replicated_part_metadata'"
        )
        node.query("SYSTEM STOP MERGES")

    # Let's bump node2 to an invalid version and verify it fails to start
    node2.replace_config(
        "/etc/clickhouse-server/config.d/metadata_version.xml", CONFIG.format(10)
    )
    node2.stop_clickhouse()
    with pytest.raises(Exception):
        node2.start_clickhouse(retry_start=False)
    assert node2.contains_in_log(
        "DB::Exception: desired_part_metadata_format_version is set to an unknown value"
    )

    # We will reset node2 to disable the feature again
    # So we can assert the parts are all still readable even though they have a newer format version than the configured setting
    node2.replace_config(
        "/etc/clickhouse-server/config.d/metadata_version.xml", CONFIG.format(0)
    )
    node2.start_clickhouse()

    # verify that the setting is updated:
    setting = node2.query(
        "SELECT value,changed FROM system.merge_tree_settings WHERE name='desired_part_metadata_format_version'"
    )
    assert setting.strip() == "0\t1"
    res, _ = zk.get("/clickhouse/tables/test_replicated_part_metadata/replicas/2/host")
    assert b"\ndesired_part_metadata_format_version:" not in res

    # Verify we still see creation time and metadata format version from existing part, even though we disabled the setting
    assert "0\t1\n" == node2.query(
        "SELECT isNull(creation_time), metadata_format_version FROM system.parts WHERE active and table='test_replicated_part_metadata'"
    )

    # If we optimize the table in this state, we should end up with a part having old metadata version, since this is the lowest common denom
    node1.query("SYSTEM START MERGES")
    node2.query("SYSTEM START MERGES")
    node1.query("OPTIMIZE TABLE test_replicated_part_metadata FINAL")
    assert "1\t0\n" == node1.query(
        "SELECT isNull(creation_time), metadata_format_version FROM system.parts WHERE active and table='test_replicated_part_metadata' ORDER BY modification_time"
    )
    assert "1\t0\n" == node2.query(
        "SELECT isNull(creation_time), metadata_format_version FROM system.parts WHERE active and table='test_replicated_part_metadata' ORDER BY modification_time"
    )

    # return node2 to original config before other tests could run
    node2.exec_in_container(
        ["rm", "/etc/clickhouse-server/config.d/metadata_version.xml"]
    )
    node2.restart_clickhouse()
    setting = node2.query(
        "SELECT value,changed FROM system.merge_tree_settings WHERE name='desired_part_metadata_format_version'"
    )
    assert setting.strip() == "0\t0"


# I want to verify that a lightweight delete will never result in creation time changing
# So if a part exists with lightweight deletes, it's guaranteed the creation time matches that of original part
def test_delete_inplace():
    node1.query(
        f"""
            CREATE TABLE test_delete_inplace(id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_delete_inplace', '1')
            ORDER BY id
            """
    )
    node1.query(
        "INSERT INTO test_delete_inplace SELECT number, number FROM numbers(20)"
    )

    assert "Compact\t0\t0\n" == node1.query(
        "SELECT part_type, level, data_version FROM system.parts where active and table='test_delete_inplace'"
    )
    ctime = node1.query(
        "SELECT creation_time FROM system.parts where active and table='test_delete_inplace'"
    )

    # Trigger a mutation via lightweight delete
    time.sleep(1)
    node1.query(
        "DELETE FROM test_delete_inplace WHERE dummy%2 == 0 SETTINGS mutations_sync=2"
    )

    # We expect that both the data version increments while level stays the same, indicating that creation time hasn't changed
    assert "Compact\t0\t1\t1\n" == node1.query(
        "SELECT part_type, level, data_version, has_lightweight_delete FROM system.parts where active and table='test_delete_inplace'"
    )
    assert ctime == node1.query(
        "SELECT creation_time FROM system.parts where active and table='test_delete_inplace'"
    )

    # Verify with another "normal" mutation too
    time.sleep(1)
    node1.query(
        "ALTER TABLE test_delete_inplace UPDATE dummy = dummy + 1 WHERE dummy%3 == 0 SETTINGS mutations_sync=2"
    )

    # Same expectation as before, data version increase, level & ctime don't change
    # Also verify the has_lightweight_delete didn't change
    assert "Compact\t0\t2\t1\n" == node1.query(
        "SELECT part_type, level, data_version, has_lightweight_delete FROM system.parts where active and table='test_delete_inplace'"
    )
    assert ctime == node1.query(
        "SELECT creation_time FROM system.parts where active and table='test_delete_inplace'"
    )


# I also want to verify that the setting `min_age_to_force_merge_seconds` will respect my creation time
# It should automatically schedule merging of parts regardless of other criteria, based on the age.
# I want to ensure that this continues to occur even if the modification time is increasing in the meantime
def test_merge_old_parts():
    # Now we will test that replicated merge trees create the new metadata as designed
    for i, node in enumerate(all_nodes):
        node.replace_config(
            "/etc/clickhouse-server/config.d/metadata_version.xml", CONFIG.format(1)
        )
        node.restart_clickhouse()
        node.query(
            f"""
            CREATE TABLE test_merge_old_parts(ts DateTime, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_merge_old_parts', '{i+1}')
            PARTITION BY toYYYYMMDD(ts) ORDER BY ts SETTINGS min_age_to_force_merge_seconds=4, merge_selecting_sleep_ms=250, min_age_to_force_merge_on_partition_only=true;
            """
        )
        assert "1\n" == node.query(
            "SELECT part_metadata_format_version FROM system.tables where name='test_merge_old_parts'"
        )
        node.query("SYSTEM STOP MERGES")

    node1.query(
        "INSERT INTO test_merge_old_parts VALUES (now()-2, 0, 345) (now()-1, 1, 456)"
    )
    assert "1\t0\t1\n" == node1.query(
        "SELECT not isNull(creation_time), level, metadata_format_version FROM system.parts WHERE active and table='test_merge_old_parts'"
    )

    for node in all_nodes:
        node.query("SYSTEM START MERGES")

    ctime = node1.query(
        "SELECT creation_time FROM system.parts WHERE active and table='test_merge_old_parts'"
    ).strip()

    # Continuously "update" the part with no-op mutations
    # The part isn't changed, meaning no merging logic will run, so creation time stays the same
    # But from the perspective of clickhouse, the data version increments, meaning it is still a "new" part in that sense
    for i in range(5):
        node1.query(
            "ALTER TABLE test_merge_old_parts UPDATE id=id+1 WHERE false SETTINGS mutations_sync=2"
        )
        time.sleep(1)

    assert "1\t1\t5\t1\n" == node1.query(
        f"SELECT creation_time >= addSeconds(toDateTime('{ctime}'), 4), level, data_version, metadata_format_version FROM system.parts WHERE active and table='test_merge_old_parts'"
    )

    # return node2 to original config before other tests could run
    node2.exec_in_container(
        ["rm", "/etc/clickhouse-server/config.d/metadata_version.xml"]
    )
    node2.restart_clickhouse()
    setting = node2.query(
        "SELECT value,changed FROM system.merge_tree_settings WHERE name='desired_part_metadata_format_version'"
    )
    assert setting.strip() == "0\t0"
