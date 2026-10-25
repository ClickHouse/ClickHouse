import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
    ],
    with_zookeeper=True,
    macros={"bound_path": "/clickhouse/tables/{database}/"},
    stay_alive = True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_warning():
    node.query("DROP DATABASE IF EXISTS testdb")
    assert node.query("SELECT name FROM system.databases where engine = 'Ordinary'") == ""
    assert node.query("SELECT count() = 0 FROM system.databases where engine = 'Ordinary'") == "1\n"

    node.query("CREATE DATABASE testdb ENGINE = Ordinary")
    assert node.query("SELECT engine FROM system.databases where name = 'testdb'") == "Ordinary\n"
    assert node.query("SELECT count() = 1 FROM system.warnings where startsWith(message, 'Server has databases (for example `testdb`) with Ordinary engine')") == "1\n"

    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
    )
    node.start_clickhouse()

    assert node.query("SELECT engine FROM system.databases where name = 'testdb'") == "Atomic\n"
    assert node.query("SELECT count() = 0 FROM system.warnings where startsWith(message, 'Server has databases (for example `testdb`) with Ordinary engine')") == "1\n"

    node.query("DROP DATABASE testdb")
    node.stop_clickhouse()
    node.start_clickhouse()


def test_conversion_keeps_queue_keeper_path():
    # The conversion ends with `RENAME DATABASE <tmp> TO <db>`, which must stay allowed for a queue
    # whose Keeper path is bound to `<db>`, whether it was loaded eagerly or as a lazy stand-in.
    databases = ["conv_eager", "conv_lazy"]
    for database in databases:
        node.query(f"DROP DATABASE IF EXISTS {database} SYNC")

    node.query("CREATE DATABASE conv_eager ENGINE = Ordinary")
    node.query("CREATE DATABASE conv_lazy ENGINE = Ordinary SETTINGS lazy_load_tables = 1")
    for database in databases:
        node.query(
            f"CREATE TABLE {database}.q (a UInt32) "
            "ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV) "
            "SETTINGS mode = 'unordered', keeper_path = '/convert{bound_path}q'"
        )

    keeper_paths_query = (
        "SELECT database, value FROM system.s3_queue_settings "
        "WHERE name = 'keeper_path' AND database IN ('conv_eager', 'conv_lazy') ORDER BY database"
    )
    keeper_paths = (
        "conv_eager\t/convert/clickhouse/tables/conv_eager/q\n"
        "conv_lazy\t/convert/clickhouse/tables/conv_lazy/q\n"
    )
    assert node.query(keeper_paths_query) == keeper_paths

    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
    )
    node.start_clickhouse()

    # A refused closing rename would leave a `.tmp_convert.conv_*` database behind.
    assert (
        node.query("SELECT name, engine FROM system.databases WHERE name LIKE '%conv%' ORDER BY name")
        == "conv_eager\tAtomic\nconv_lazy\tAtomic\n"
    )
    assert node.query(keeper_paths_query) == keeper_paths

    for database in databases:
        node.query(f"DROP DATABASE {database} SYNC")


def test_failed_queue_load_blocks_rename_database():
    # A queue whose load failed has no storage object, but the next startup loads it under the new name.
    node.query("DROP DATABASE IF EXISTS failed_db SYNC")
    node.query("CREATE DATABASE failed_db ENGINE = Atomic")
    node.query(
        "CREATE TABLE failed_db.q (a UInt32) "
        "ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV) "
        "SETTINGS mode = 'unordered', keeper_path = '/failed{bound_path}q'"
    )

    macros = "/etc/clickhouse-server/conf.d/macros.xml"
    node.stop_clickhouse()
    node.exec_in_container(["bash", "-c", f"mv {macros} /tmp/macros.xml"], user="root")
    node.start_clickhouse()

    assert "No macro 'bound_path'" in node.query_and_get_error("SELECT * FROM failed_db.q")
    node.query_and_get_error("RENAME DATABASE failed_db TO failed_db_renamed")

    node.stop_clickhouse()
    node.exec_in_container(["bash", "-c", f"mv /tmp/macros.xml {macros}"], user="root")
    node.start_clickhouse()

    assert (
        node.query(
            "SELECT database, value FROM system.s3_queue_settings "
            "WHERE name = 'keeper_path' AND database LIKE 'failed_db%'"
        )
        == "failed_db\t/failed/clickhouse/tables/failed_db/q\n"
    )
    node.query("DROP DATABASE failed_db SYNC")
