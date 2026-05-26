import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_metric_value(instance, metric_name):
    result = instance.query(
        f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
    ).strip()
    return int(result) if result else 0


def test_freeze_recovery_skips_broken_disk(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/105719.
    #
    # FREEZE recovers an empty/missing shadow/increment.txt by scanning every
    # configured disk for the maximum numeric shadow/<N> directory, so the next
    # unnamed FREEZE allocates above it. A broken disk cannot be scanned, but it
    # is a routine state: it must be skipped (with a log message) rather than make
    # every FREEZE on the server fail. Once the disk is healthy again, recovery
    # scans it and allocates above the directories it holds.
    try:
        node.query("DROP TABLE IF EXISTS t_cold SYNC")
        node.query("DROP TABLE IF EXISTS t_warm SYNC")

        # A table on the broken-able cold disk, and one on the healthy default disk.
        node.query(
            "CREATE TABLE t_cold (id UInt64) ENGINE = MergeTree ORDER BY id "
            "SETTINGS storage_policy = 'cold_policy'"
        )
        node.query("INSERT INTO t_cold VALUES (1), (2), (3)")
        node.query("CREATE TABLE t_warm (id UInt64) ENGINE = MergeTree ORDER BY id")
        node.query("INSERT INTO t_warm VALUES (1), (2), (3)")

        # Create a numeric backup directory on the cold disk: cold/shadow/7/.
        node.query("ALTER TABLE t_cold FREEZE WITH NAME '7'")
        assert (
            node.exec_in_container(
                ["bash", "-c", "test -d /var/lib/clickhouse/cold/shadow/7 && echo yes || echo no"]
            ).strip()
            == "yes"
        )

        # Plant the broken counter state: an empty default shadow/increment.txt.
        node.exec_in_container(
            ["bash", "-c", "mkdir -p /var/lib/clickhouse/shadow && : > /var/lib/clickhouse/shadow/increment.txt"]
        )

        # Break the cold disk by moving its directory away; the disk checker
        # thread marks it broken within local_disk_check_period_ms.
        node.exec_in_container(
            ["bash", "-c", "mv /var/lib/clickhouse/cold /var/lib/clickhouse/cold_moved"]
        )
        wait_condition(
            func=lambda: get_metric_value(node, "BrokenDisks"),
            condition=lambda value: value >= 1,
            max_attempts=30,
            delay=1,
        )

        # An unnamed FREEZE on the healthy default disk must SUCCEED even though
        # an unrelated disk is broken: recovery skips the broken disk and logs an
        # info message. Before this fix the recovery threw ABORTED, so every
        # FREEZE on the server failed for as long as any disk stayed broken.
        node.rotate_logs()
        node.query("ALTER TABLE t_warm FREEZE")
        assert (
            node.exec_in_container(
                ["bash", "-c", "test -s /var/lib/clickhouse/shadow/increment.txt && echo yes || echo no"]
            ).strip()
            == "yes"
        )
        assert node.contains_in_log("is broken and was skipped while recovering")

        # Restore the disk and restart the server so it comes up healthy (a
        # moved-away local disk recovers on restart, see test_disk_checker).
        # The disk checker may have recreated an empty cold/ while it was broken,
        # so drop that before moving the original data directory back.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/cold && mv /var/lib/clickhouse/cold_moved /var/lib/clickhouse/cold",
            ]
        )
        node.restart_clickhouse()
        assert node.query("SELECT count() FROM t_cold").strip() == "3"

        # With every disk healthy, recovery scans cold/shadow/7 and allocates a
        # higher id, so the new backup does not reuse the existing directory.
        node.exec_in_container(
            ["bash", "-c", ": > /var/lib/clickhouse/shadow/increment.txt"]
        )
        node.query("ALTER TABLE t_cold FREEZE")
        numeric_dirs = node.exec_in_container(
            [
                "bash",
                "-c",
                "ls /var/lib/clickhouse/cold/shadow | grep -E '^[0-9]+$' | sort -n | tr '\\n' ' '",
            ]
        ).split()
        # cold/shadow/7 must survive, and the new backup must be a higher id.
        assert "7" in numeric_dirs, numeric_dirs
        assert max(int(d) for d in numeric_dirs) > 7, numeric_dirs
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "test -d /var/lib/clickhouse/cold_moved && "
                "{ rm -rf /var/lib/clickhouse/cold; mv /var/lib/clickhouse/cold_moved /var/lib/clickhouse/cold; } "
                "|| true",
            ]
        )
        node.query("DROP TABLE IF EXISTS t_cold SYNC")
        node.query("DROP TABLE IF EXISTS t_warm SYNC")
