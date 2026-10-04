import os

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
nodes = {}
for name in ("initiator", "unavailable", "available"):
    nodes[name] = cluster.add_instance(
        name,
        main_configs=["configs/cluster.xml"],
        user_configs=["configs/positional.xml"],
        with_zookeeper=True,
        stay_alive=True,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_modify_projection_requires_local_table_before_cluster_dispatch(started_cluster):
    initiator = nodes["initiator"]
    unavailable = nodes["unavailable"]
    available = nodes["available"]

    initiator.query("CREATE DATABASE projection_cluster_db ON CLUSTER projection_cluster")
    create_table = (
        "CREATE TABLE projection_cluster_db.t (a UInt64, b String, "
        "PROJECTION pp (SELECT b, a GROUP BY 1, 2)) "
        "ENGINE = MergeTree ORDER BY a"
    )
    for node in (unavailable, available):
        node.query(create_table)

    positional_config = "/etc/clickhouse-server/users.d/positional.xml"
    unavailable.exec_in_container(["rm", positional_config])
    try:
        unavailable.restart_clickhouse()
        count = (
            "SELECT count() FROM system.projections "
            "WHERE database = 'projection_cluster_db' AND table = 't'"
        )
        assert unavailable.query(count).strip() == "0"
        assert available.query(count).strip() == "1"

        error = initiator.query_and_get_error(
            "ALTER TABLE projection_cluster_db.t ON CLUSTER projection_cluster "
            "MODIFY PROJECTION pp (SELECT b, a GROUP BY 1, 2) "
            "WITH SETTINGS (index_granularity = 64)",
            settings={"distributed_ddl_task_timeout": 15},
        )
        for node in (unavailable, available):
            assert "index_granularity = 64" not in node.query(
                "SHOW CREATE TABLE projection_cluster_db.t"
            )
        assert "does not exist on this host" in error

        # The initiator can now validate the existing definition. The unavailable worker
        # must preserve that definition and apply the settings-only change as well.
        initiator.query(create_table)
        initiator.query(
            "ALTER TABLE projection_cluster_db.t ON CLUSTER projection_cluster "
            "MODIFY PROJECTION pp (SELECT b, a GROUP BY 1, 2) "
            "WITH SETTINGS (index_granularity = 128)"
        )
        for node in (initiator, unavailable, available):
            assert "index_granularity = 128" in node.query(
                "SHOW CREATE TABLE projection_cluster_db.t"
            )
        assert unavailable.query(count).strip() == "0"

        error = initiator.query_and_get_error(
            "ALTER TABLE projection_cluster_db.t ON CLUSTER projection_cluster "
            "MODIFY PROJECTION pp (SELECT a, b GROUP BY 1, 2) "
            "WITH SETTINGS (index_granularity = 256)"
        )
        assert "only the WITH SETTINGS clause may be changed" in error

        error = initiator.query_and_get_error(
            "ALTER TABLE projection_cluster_db.t ON CLUSTER projection_cluster "
            "MODIFY PROJECTION pp (SELECT b, a GROUP BY 1, 2) "
            "WITH SETTINGS (parts_to_throw_insert = 1)"
        )
        assert "not allowed for projections" in error
        for node in (initiator, unavailable, available):
            assert "index_granularity = 128" in node.query(
                "SHOW CREATE TABLE projection_cluster_db.t"
            )
    finally:
        unavailable.copy_file_to_container(
            os.path.join(os.path.dirname(__file__), "configs/positional.xml"),
            positional_config,
        )
        unavailable.restart_clickhouse()

    assert unavailable.query(count).strip() == "1"
    assert "index_granularity = 128" in unavailable.query(
        "SHOW CREATE TABLE projection_cluster_db.t"
    )


def test_legacy_create_as_does_not_split_on_unavailable_projection(started_cluster):
    initiator = nodes["initiator"]
    unavailable = nodes["unavailable"]
    available = nodes["available"]
    source = "default.legacy_projection_copy_source"
    destination = "default.legacy_projection_copy_destination"
    clone_destination = "default.legacy_projection_clone_destination"
    plain_source = "default.legacy_projection_plain_source"
    plain_destination = "default.legacy_projection_plain_destination"
    projection_count = (
        "SELECT count() FROM system.projections "
        "WHERE database = 'default' AND table = 'legacy_projection_copy_source'"
    )

    for node in nodes.values():
        node.query(f"DROP TABLE IF EXISTS {destination} SYNC")
        node.query(f"DROP TABLE IF EXISTS {clone_destination} SYNC")
        node.query(f"DROP TABLE IF EXISTS {source} SYNC")
        node.query(f"DROP TABLE IF EXISTS {plain_destination} SYNC")
        node.query(f"DROP TABLE IF EXISTS {plain_source} SYNC")
        node.query(
            f"CREATE TABLE {source} (a UInt64, b String, "
            "PROJECTION pp (SELECT b, a GROUP BY 1, 2)) "
            "ENGINE = MergeTree ORDER BY a"
        )

    positional_config = "/etc/clickhouse-server/users.d/positional.xml"
    unavailable.exec_in_container(["rm", positional_config])
    try:
        unavailable.restart_clickhouse()
        assert unavailable.query(projection_count).strip() == "0"
        assert available.query(projection_count).strip() == "1"

        legacy_settings = {
            "distributed_ddl_entry_format_version": 1,
            "distributed_ddl_task_timeout": 30,
            "distributed_ddl_output_mode": "throw",
        }
        error = initiator.query_and_get_error(
            f"CREATE TABLE {destination} ON CLUSTER projection_cluster AS {source} "
            "ENGINE = MergeTree ORDER BY a",
            settings=legacy_settings,
        )
        assert "Cannot copy projections with CREATE TABLE" in error, error
        published = {
            name: node.query(f"EXISTS TABLE {destination}").strip()
            for name, node in nodes.items()
        }
        assert published == dict.fromkeys(nodes, "0"), published

        error = initiator.query_and_get_error(
            f"CREATE TABLE {destination} ON CLUSTER projection_cluster AS {source} "
            "ENGINE = MergeTree ORDER BY a",
            settings={**legacy_settings, "distributed_ddl_entry_format_version": 2},
        )
        assert "Cannot copy projections with CREATE TABLE" in error, error
        for node in nodes.values():
            assert node.query(f"EXISTS TABLE {destination}").strip() == "0"

        # `CLONE AS` still needs the worker's local source parts, so it cannot use
        # initiator normalization in a legacy multi-host DDL entry.
        error = initiator.query_and_get_error(
            f"CREATE TABLE {clone_destination} ON CLUSTER projection_cluster "
            f"CLONE AS {source}",
            settings=legacy_settings,
        )
        assert "CLONE AS" in error and "multi-host cluster" in error, error
        for node in nodes.values():
            assert node.query(f"EXISTS TABLE {clone_destination}").strip() == "0"

        # A source without projections can be normalized once and copied to every host,
        # even when that source table exists only on the initiator.
        initiator.query(
            f"CREATE TABLE {plain_source} (a UInt64) ENGINE = MergeTree ORDER BY a"
        )
        initiator.query(
            f"CREATE TABLE {plain_destination} ON CLUSTER projection_cluster "
            f"AS {plain_source} ENGINE = MergeTree ORDER BY a",
            settings=legacy_settings,
        )
        for node in nodes.values():
            assert node.query(f"EXISTS TABLE {plain_destination}").strip() == "1"
    finally:
        unavailable.copy_file_to_container(
            os.path.join(os.path.dirname(__file__), "configs/positional.xml"),
            positional_config,
        )
        unavailable.restart_clickhouse()
        for node in nodes.values():
            node.query(f"DROP TABLE IF EXISTS {destination} SYNC")
            node.query(f"DROP TABLE IF EXISTS {clone_destination} SYNC")
            node.query(f"DROP TABLE IF EXISTS {source} SYNC")
            node.query(f"DROP TABLE IF EXISTS {plain_destination} SYNC")
            node.query(f"DROP TABLE IF EXISTS {plain_source} SYNC")
