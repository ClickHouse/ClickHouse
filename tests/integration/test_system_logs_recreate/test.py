# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node_default",
    main_configs=["configs/config.d/storage_configuration.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_logs_recreate():
    system_logs = [
        # enabled by default
        "query_log",
        "query_metric_log",
        "query_thread_log",
        "part_log",
        "trace_log",
        "metric_log",
        "error_log",
    ]

    try:
        node.query("SYSTEM FLUSH LOGS")
        for table in system_logs:
            assert "ENGINE = MergeTree" in node.query(
                f"SHOW CREATE TABLE system.{table}"
            )
            assert "ENGINE = `Null`" not in node.query(
                f"SHOW CREATE TABLE system.{table}"
            )
            assert (
                len(
                    node.query(f"SHOW TABLES FROM system LIKE '{table}%'")
                    .strip()
                    .split("\n")
                )
                == 1
            )

        # NOTE: we use zzz- prefix to make it the last file,
        # so that it will be applied last.
        for table in system_logs:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"""echo "
            <clickhouse>
                <{table}>
                    <engine>ENGINE = Null</engine>
                    <partition_by remove='remove'/>
                </{table}>
            </clickhouse>
            " > /etc/clickhouse-server/config.d/zzz-override-{table}.xml
            """,
                ]
            )

        node.restart_clickhouse()
        node.query("SYSTEM FLUSH LOGS")
        for table in system_logs:
            assert "ENGINE = MergeTree" not in node.query(
                f"SHOW CREATE TABLE system.{table}"
            )
            assert "ENGINE = `Null`" in node.query(f"SHOW CREATE TABLE system.{table}")
            assert (
                len(
                    node.query(f"SHOW TABLES FROM system LIKE '{table}%'")
                    .strip()
                    .split("\n")
                )
                == 2
            )

        # apply only storage_policy for all system tables
        for table in system_logs:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"""echo "
            <clickhouse>
                <{table}>
                    <storage_policy>system_tables</storage_policy>
                </{table}>
            </clickhouse>
            " > /etc/clickhouse-server/config.d/zzz-override-{table}.xml
            """,
                ]
            )
        node.restart_clickhouse()
        node.query("SYSTEM FLUSH LOGS")
        import logging

        for table in system_logs:
            create_table_sql = node.query(
                f"SHOW CREATE TABLE system.{table} FORMAT TSVRaw"
            )
            logging.debug(
                "With storage policy, SHOW CREATE TABLE system.%s is: %s",
                table,
                create_table_sql,
            )
            assert "ENGINE = MergeTree" in create_table_sql
            assert "ENGINE = `Null`" not in create_table_sql
            assert "SETTINGS storage_policy = 'system_tables'" in create_table_sql
            assert (
                len(
                    node.query(f"SHOW TABLES FROM system LIKE '{table}%'")
                    .strip()
                    .split("\n")
                )
                == 3
            )

        for table in system_logs:
            node.exec_in_container(
                ["rm", f"/etc/clickhouse-server/config.d/zzz-override-{table}.xml"]
            )

        node.restart_clickhouse()
        node.query("SYSTEM FLUSH LOGS")
        for table in system_logs:
            assert "ENGINE = MergeTree" in node.query(
                f"SHOW CREATE TABLE system.{table}"
            )
            assert "ENGINE = `Null`" not in node.query(
                f"SHOW CREATE TABLE system.{table}"
            )
            assert (
                len(
                    node.query(f"SHOW TABLES FROM system LIKE '{table}%'")
                    .strip()
                    .split("\n")
                )
                == 4
            )

        node.query("SYSTEM FLUSH LOGS")
        # Ensure that there was no superfluous RENAME's
        # IOW that the table created only when the structure is indeed different.
        for table in system_logs:
            assert (
                len(
                    node.query(f"SHOW TABLES FROM system LIKE '{table}%'")
                    .strip()
                    .split("\n")
                )
                == 4
            )
    finally:
        for table in system_logs:
            for syffix in range(3):
                node.query(f"DROP TABLE IF EXISTS system.{table}_{syffix} sync")


def test_drop_system_log():
    node.exec_in_container(
        [
            "bash",
            "-c",
            """echo "
        <clickhouse>
            <query_log>
                <flush_interval_milliseconds replace=\\"replace\\">1000000</flush_interval_milliseconds>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    node.restart_clickhouse()
    node.query("select 1")
    node.query("system flush logs")
    node.query("select 2")
    node.query("system flush logs")
    assert node.query("select count() >= 2 from system.query_log") == "1\n"

    node.query("drop table system.query_log sync")
    node.query("select 3")
    node.query("system flush logs")
    assert node.query("select count() >= 1 from system.query_log") == "1\n"

    node.query("drop table system.query_log sync")
    node.restart_clickhouse()
    node.query("system flush logs")
    assert (
        node.query("select count() >= 0 from system.query_log") == "1\n"
    )  # we check that query_log just exists

    node.exec_in_container(
        ["rm", "/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )
    node.restart_clickhouse()


def test_system_log_rotation_keeps_row_policy():
    # Schema rotation renames the active log aside to system.query_log_<N> and recreates the
    # replacement under system.query_log. That swap only replaces the storage behind the name, so a
    # row policy on system.query_log must stay on it and keep filtering the new active log. If the
    # policy followed the data to the archived name instead, the new active log would be unfiltered.
    try:
        node.query(
            "CREATE ROW POLICY qlp ON system.query_log FOR SELECT USING type = 'QueryFinish' TO ALL"
        )
        node.query("SELECT 'before rotation'")
        node.query("SYSTEM FLUSH LOGS")
        assert (
            node.query(
                "SELECT count() = 0 FROM system.query_log WHERE type != 'QueryFinish'"
            )
            == "1\n"
        )

        # A different engine makes the stored CREATE query differ, which is what triggers rotation.
        # <engine> carries PARTITION BY/ORDER BY itself, and the server refuses to start if the
        # sibling <partition_by> is also set, so the one from config.xml has to be removed here.
        node.exec_in_container(
            [
                "bash",
                "-c",
                """echo "
        <clickhouse>
            <query_log>
                <engine>ENGINE = MergeTree ORDER BY (event_time)</engine>
                <partition_by remove='remove'/>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/zzz-override-query_log-policy.xml
        """,
            ]
        )
        node.restart_clickhouse()
        node.query("SELECT 'after rotation'")
        node.query("SYSTEM FLUSH LOGS")

        # The rotation really happened: the archived table exists next to the recreated one.
        assert node.query("EXISTS TABLE system.query_log_0") == "1\n"
        # The policy stayed on the stable name rather than following the data to the archive.
        assert (
            node.query(
                "SELECT table FROM system.row_policies WHERE short_name = 'qlp' AND database = 'system'"
            )
            == "query_log\n"
        )
        # And it still filters the new active log, which really does have rows.
        assert node.query("SELECT count() > 0 FROM system.query_log") == "1\n"
        assert (
            node.query(
                "SELECT count() = 0 FROM system.query_log WHERE type != 'QueryFinish'"
            )
            == "1\n"
        )
    finally:
        node.query("DROP ROW POLICY IF EXISTS qlp ON system.query_log")
        node.exec_in_container(
            [
                "rm",
                "-f",
                "/etc/clickhouse-server/config.d/zzz-override-query_log-policy.xml",
            ]
        )
        node.restart_clickhouse()
        for suffix in range(2):
            node.query(f"DROP TABLE IF EXISTS system.query_log_{suffix} SYNC")
