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
            assert "ENGINE = Null" not in node.query(
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
            assert "ENGINE = Null" in node.query(f"SHOW CREATE TABLE system.{table}")
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
            assert "ENGINE = Null" not in create_table_sql
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
            assert "ENGINE = Null" not in node.query(
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
            f"""echo "
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
        ["rm", f"/etc/clickhouse-server/config.d/yyy-override-query_log.xml"]
    )
    node.restart_clickhouse()
