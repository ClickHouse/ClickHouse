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
