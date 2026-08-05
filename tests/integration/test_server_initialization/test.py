import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # Disable `with_remote_database_disk` as the test uses the local disk to store metadata files.
        instance = cluster.add_instance(
            "dummy",
            clickhouse_path_dir="clickhouse_path",
            stay_alive=True,
            with_remote_database_disk=False,
        )
        cluster.start()

        cluster_fail = ClickHouseCluster(__file__, name="fail")
        instance_fail = cluster_fail.add_instance(
            "dummy_fail",
            clickhouse_path_dir="clickhouse_path_fail",
            with_remote_database_disk=False,
        )
        with pytest.raises(Exception):
            cluster_fail.start()
        cluster_fail.shutdown()  # cleanup

        yield cluster

    finally:
        cluster.shutdown()


def test_sophisticated_default(started_cluster):
    instance = started_cluster.instances["dummy"]
    instance.query("TRUNCATE TABLE sophisticated_default")
    instance.query("INSERT INTO sophisticated_default (c) VALUES (0)")
    assert instance.query("SELECT a, b, c FROM sophisticated_default") == "3\t9\t0\n"


def test_partially_dropped_tables(started_cluster):
    instance = started_cluster.instances["dummy"]
    assert (
        instance.exec_in_container(
            ["bash", "-c", "find /var/lib/clickhouse/*/default -name *.sql* | sort"],
            privileged=True,
            user="root",
        )
        == "/var/lib/clickhouse/metadata/default/should_be_restored.sql\n"
        "/var/lib/clickhouse/metadata/default/sophisticated_default.sql\n"
    )
    assert instance.query("SELECT n FROM should_be_restored") == "1\n2\n3\n"
    assert (
        instance.query(
            "SELECT count() FROM system.tables WHERE name='should_be_dropped'"
        )
        == "0\n"
    )
