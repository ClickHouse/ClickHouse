import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import set_convert_flags

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

database_name = "modify_engine_broken_disk"

LOOPED_DISK_XML = """<clickhouse>
    <storage_configuration>
        <disks>
            <looped>
                <type>local</type>
                <path>/var/lib/clickhouse/loop_a/disk/</path>
            </looped>
        </disks>
        <policies>
            <with_looped>
                <volumes>
                    <main><disk>default</disk></main>
                    <broken><disk>looped</disk></broken>
                </volumes>
            </with_looped>
        </policies>
    </storage_configuration>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # A `local` disk whose path is a symlink loop is broken.
        ch1.exec_in_container(
            [
                "bash",
                "-c",
                "ln -sfn loop_b /var/lib/clickhouse/loop_a && ln -sfn loop_a /var/lib/clickhouse/loop_b",
            ]
        )
        ch1.exec_in_container(
            [
                "bash",
                "-c",
                f"cat > /etc/clickhouse-server/config.d/looped_disk.xml << 'EOF'\n{LOOPED_DISK_XML}\nEOF",
            ]
        )
        ch1.restart_clickhouse()
        assert (
            ch1.query("SELECT is_broken FROM system.disks WHERE name = 'looped'")
            == "1\n"
        )
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return ch1.query(database=database_name, sql=query)


def engine_of(table):
    return q(
        f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = '{table}'"
    ).strip()


def test_broken_disk_outside_storage_policy(started_cluster):
    # A broken disk that the table's storage policy does not use holds none of its parts,
    # so neither conversion route fails on it.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q("CREATE TABLE attached ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q("INSERT INTO attached SELECT number FROM numbers(10)")
    q("DETACH TABLE attached")
    q("ATTACH TABLE attached AS REPLICATED")
    assert engine_of("attached") == "ReplicatedMergeTree"
    assert q("SELECT count() FROM attached").strip() == "10"

    q("CREATE TABLE flagged ( A Int64 ) ENGINE = MergeTree ORDER BY A")
    q("INSERT INTO flagged SELECT number FROM numbers(10)")
    set_convert_flags(ch1, database_name, ["flagged"])
    ch1.restart_clickhouse()
    assert engine_of("flagged") == "ReplicatedMergeTree"
    assert q("SELECT count() FROM flagged").strip() == "10"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_broken_disk_in_storage_policy(started_cluster):
    # A broken disk in the policy may hold parts with transaction metadata that would load into
    # the replicated table once the disk is back, so the conversion is still refused.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q(
        "CREATE TABLE on_looped ( A Int64 ) ENGINE = MergeTree ORDER BY A "
        "SETTINGS storage_policy = 'with_looped'"
    )
    q("INSERT INTO on_looped SELECT number FROM numbers(10)")
    q("DETACH TABLE on_looped")
    assert "CANNOT_RESTORE_TABLE" in ch1.query_and_get_error(
        "ATTACH TABLE on_looped AS REPLICATED", database=database_name
    )
    q("ATTACH TABLE on_looped")
    assert engine_of("on_looped") == "MergeTree"
    assert q("SELECT count() FROM on_looped").strip() == "10"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
