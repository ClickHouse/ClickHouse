import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/display_name.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


DEFAULT_VALUE = "424242"
CHANGED_VALUE = "434343"


def test_system_reload_config_with_global_context(start_cluster):
    # When running the this test multiple times, make sure failure of one test won't cause the failure of every subsequent tests
    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/display_name.xml", CHANGED_VALUE, DEFAULT_VALUE
    )
    instance.restart_clickhouse()

    assert DEFAULT_VALUE == instance.query("SELECT displayName()").strip()

    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/display_name.xml", DEFAULT_VALUE, CHANGED_VALUE
    )

    instance.query("SYSTEM RELOAD CONFIG")

    assert CHANGED_VALUE == instance.query("SELECT displayName()").strip()
