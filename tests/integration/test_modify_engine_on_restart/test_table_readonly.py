import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import get_table_path, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_ro_setting"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return ch1.query(database=database_name, sql=query)


def engine_of(table):
    return q(
        f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = '{table}'"
    ).strip()


def create_table_query_of(table):
    return q(
        f"SELECT create_table_query FROM system.tables WHERE database = '{database_name}' AND name = '{table}'"
    )


def test_convert_to_replicated_skips_a_readonly_table(started_cluster):
    # `table_readonly` is not supported for `ReplicatedMergeTree`, and a converted table keeps the
    # settings of the table it was converted from, so the conversion would produce a table in that
    # unsupported state. It is skipped instead: the table keeps loading and serving, and the flag is
    # kept, so the conversion runs on the next start once the setting is gone. Throwing would take
    # the table down with the whole database load, and the setting could then not be reset at all.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q("CREATE TABLE to_convert ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q("INSERT INTO to_convert SELECT number FROM numbers(10)")
    # A readonly table refuses writes, so the rows go in before the setting is put on it.
    q("ALTER TABLE to_convert MODIFY SETTING table_readonly = 1")

    set_convert_flags(ch1, database_name, ["to_convert"])
    table_data_path = get_table_path(ch1, "to_convert", database_name)

    ch1.restart_clickhouse()

    assert engine_of("to_convert") == "MergeTree"
    assert q("SELECT count() FROM to_convert").strip() == "10"
    assert "convert_to_replicated" in ch1.exec_in_container(
        ["bash", "-c", f"ls {table_data_path}"]
    )

    # Once the setting is gone the kept flag makes the conversion happen on the next start.
    q("ALTER TABLE to_convert RESET SETTING table_readonly")
    ch1.restart_clickhouse()

    assert engine_of("to_convert") == "ReplicatedMergeTree"
    assert q("SELECT count() FROM to_convert").strip() == "10"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_replicated_table_carrying_the_setting_loads(started_cluster):
    # A `ReplicatedMergeTree` whose stored definition carries `table_readonly = 1` - which the
    # conversion above produced before it learned to skip such a table - has to keep loading, both on
    # startup and through a short `ATTACH TABLE t`, and `ALTER TABLE ... RESET SETTING
    # table_readonly` is the way out of that state. Refusing the load left the table unreachable:
    # a `DETACH` was one-way and a backup of it could not be restored.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q(
        "CREATE TABLE legacy ( A Int64 ) ENGINE = "
        "ReplicatedMergeTree('/clickhouse/tables/{database}/legacy', '{replica}') ORDER BY A"
    )
    q("INSERT INTO legacy SELECT number FROM numbers(10)")

    metadata_path = q(
        f"SELECT metadata_path FROM system.tables WHERE database = '{database_name}' AND name = 'legacy'"
    ).strip()
    # `metadata_path` is reported relative to the server's data directory.
    if not metadata_path.startswith("/"):
        metadata_path = "/var/lib/clickhouse/" + metadata_path

    # The setting can no longer be introduced through SQL, so the definition of an older server is
    # planted directly, which is the only way a table can carry it now.
    ch1.stop_clickhouse()
    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"sed -i 's/SETTINGS index_granularity = 8192/SETTINGS index_granularity = 8192, table_readonly = 1/' {metadata_path}",
        ]
    )
    ch1.start_clickhouse()

    assert "table_readonly = 1" in create_table_query_of("legacy")
    assert q("SELECT count() FROM legacy").strip() == "10"

    # The short `ATTACH TABLE t` replays the same stored definition.
    q("DETACH TABLE legacy")
    q("ATTACH TABLE legacy")
    assert q("SELECT count() FROM legacy").strip() == "10"

    # And the table can be taken out of that state.
    q("ALTER TABLE legacy RESET SETTING table_readonly")
    assert "table_readonly" not in create_table_query_of("legacy")
    assert q("SELECT count() FROM legacy").strip() == "10"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
