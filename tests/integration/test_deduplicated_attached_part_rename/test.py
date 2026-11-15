import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "dedup_attach"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return ch1.query(database=database_name, sql=query)


def test_deduplicated_attached_part_renamed_after_attach(started_cluster):
    ch1.query(f"CREATE DATABASE {database_name}")

    q(
        "CREATE TABLE dedup (id UInt32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/dedup_attach/dedup/s1', 'r1') ORDER BY id;"
    )
    q("INSERT INTO dedup VALUES (1),(2),(3);")

    table_data_path = q(
        "SELECT data_paths FROM system.tables WHERE database=currentDatabase() AND table='dedup'"
    ).strip("'[]\n")

    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"cp -r {table_data_path}/all_0_0_0 {table_data_path}/detached/all_0_0_0",
        ]
    )
    # Part is attached as all_1_1_0
    q("ALTER TABLE dedup ATTACH PART 'all_0_0_0'")

    assert 2 == int(
        q(
            f"SELECT count() FROM system.parts WHERE database='{database_name}' AND table = 'dedup'"
        ).strip()
    )

    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"cp -r {table_data_path}/all_1_1_0 {table_data_path}/detached/all_1_1_0",
        ]
    )
    # Part is deduplicated and not attached
    q("ALTER TABLE dedup ATTACH PART 'all_1_1_0'")

    assert 2 == int(
        q(
            f"SELECT count() FROM system.parts WHERE database='{database_name}' AND table = 'dedup'"
        ).strip()
    )
    assert 1 == int(
        q(
            f"SELECT count() FROM system.detached_parts WHERE database='{database_name}' AND table = 'dedup'"
        ).strip()
    )
    # Check that it is not 'attaching_all_1_1_0'
    assert (
        "all_1_1_0"
        == q(
            f"SELECT name FROM system.detached_parts WHERE database='{database_name}' AND table = 'dedup'"
        ).strip()
    )

    q("DROP TABLE dedup")
    q("SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/dedup_attach/dedup/s1'")
    ch1.query(f"DROP DATABASE {database_name}")
