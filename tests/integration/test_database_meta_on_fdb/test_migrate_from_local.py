import pytest
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent
import os.path


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__, name="migrate")
        node = cluster.add_instance(
            'node',
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start(destroy_dirs=True)
        yield cluster

    finally:
        cluster.shutdown()

def toggle_fdb(enable, started_cluster):
    node = started_cluster.instances["node"]
    if enable:
        with open(os.path.dirname(__file__) + "/configs/foundationdb.xml", "r") as f:
            node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())
    else:
        node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", "<clickhouse></clickhouse>")

def test_migrate_from_local(started_cluster):
    db_name = "test_migrate"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"CREATE DATABASE {db_name}_ignore ENGINE Memory")

    node.stop_clickhouse()

    # First boot with fdb
    toggle_fdb(True,started_cluster)
    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
    # Unsupport database will be ignored
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_ignore'").strip() == "0"

    # Disable fdb and change local data
    node.stop_clickhouse()
    toggle_fdb(False, started_cluster)
    node.start_clickhouse()

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_ignore'").strip() == "1"
    node.query(f"CREATE DATABASE {db_name}_ignore_2");
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_ignore_2'").strip() == "1"

    # Second boot with fdb. Local data should not be uploaded.
    node.stop_clickhouse()
    toggle_fdb(True, started_cluster)
    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_ignore'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_ignore_2'").strip() == "0"

def test_migrate_from_local_when_fdb_down(started_cluster):
    db_name = "test_migrate"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")

    node.stop_clickhouse()

    # First boot with fdb, but fdb is down
    toggle_fdb(True, started_cluster)
    started_cluster.stop_fdb()
    with pytest.raises(Exception, match="Cannot start ClickHouse"):
        node.start_clickhouse()
    assert node.contains_in_log("Operation aborted because the transaction timed out")

    # Disable fdb and change local data
    node.stop_clickhouse()
    toggle_fdb(False, started_cluster)
    node.start_clickhouse()

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
    node.query(f"CREATE DATABASE {db_name}_2");
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_2'").strip() == "1"

    # Second boot with fdb
    node.stop_clickhouse()
    toggle_fdb(True, started_cluster)
    started_cluster.start_fdb()
    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}_2'").strip() == "1"