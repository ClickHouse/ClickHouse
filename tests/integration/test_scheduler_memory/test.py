# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def clear_workloads_and_resources():
    node.query(
        f"""
        drop workload if exists production;
        drop workload if exists development;
        drop workload if exists admin;
        drop workload if exists all;
        drop resource if exists memory;
    """
    )
    yield


def test_create_workload():
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='1G';
        create workload admin in all settings precedence=-1;
        create workload production in all settings precedence=1, weight=9;
        create workload development in all settings precedence=1, weight=1;
    """
    )

    def do_checks():
        # Check that allocation_queue nodes are created for memory resource
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/admin/%' and type='allocation_queue'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/production/%' and type='allocation_queue'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/development/%' and type='allocation_queue'"
            )
            == "1\n"
        )
        # Check that allocation_limit node is created with max_memory setting
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/all/%' and type='allocation_limit' and resource='memory'"
            )
            == "1\n"
        )

    do_checks()
    node.restart_clickhouse()  # Check that workloads persist
    do_checks()


def test_reserve_memory():
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='10Gi';
        create workload production in all;
        create workload development in all;
    """
    )

    # Run simple queries with workload settings
    node.query(
        "select count(*) from numbers(1000000) settings workload='production', reserve_memory='128Mi'",
        query_id="test_production",
    )
    node.query(
        "select count(*) from numbers(1000000) settings workload='development', reserve_memory='128Mi'",
        query_id="test_development",
    )

    node.query("SYSTEM FLUSH LOGS")

