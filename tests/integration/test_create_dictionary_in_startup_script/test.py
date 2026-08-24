
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "config/startup_scripts.xml",
    ],
    user_configs=[
        "config/users.xml",
    ],
    stay_alive=True,
    with_minio=True,
    macros={"shard": 1, "replica": 1},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_startup_script_succeeded():
    STATE_SUCCESS = "1"
    assert node.query(
        """
        SELECT value
        FROM system.metrics
        WHERE name = 'StartupScriptsExecutionState'
        """
    ).strip() == STATE_SUCCESS

    assert int(
        node.query(
            """
            SELECT count()
            FROM system.custom_metrics
            """
        ).strip()
    ) > 0


def test_create_dictionary_in_startup_script(start_cluster):
    check_startup_script_succeeded()

    # Startup scripts re-run on every restart, and `drop_dictionary` must
    # target the same dictionary `create_dictionary` creates - otherwise the
    # stale dictionary survives the drop and the second run's
    # `CREATE DICTIONARY` fails with `DICTIONARY_ALREADY_EXISTS`, which a
    # single-boot check can't catch.
    node.restart_clickhouse()
    check_startup_script_succeeded()
