import threading
import time
import uuid
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/query_slots.xml"], with_zookeeper=True, stay_alive=True
)
legacy = cluster.add_instance("legacy", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    for instance in (node, legacy):
        instance.query("DROP DATABASE IF EXISTS rmv_slots SYNC")
        instance.query("DROP TABLE IF EXISTS mv SYNC")
        instance.query("DROP TABLE IF EXISTS queued SYNC")
        instance.query("DROP WORKLOAD IF EXISTS updated")
        instance.query("DROP WORKLOAD IF EXISTS all")
        instance.query("DROP RESOURCE IF EXISTS query")


def wait_query(instance, query, expected, timeout=30):
    deadline = time.monotonic() + timeout
    actual = None
    while time.monotonic() < deadline:
        actual = instance.query(query).strip()
        if actual == str(expected):
            return
        time.sleep(0.1)
    assert actual == str(expected), (query, actual, expected)


def metric(instance, name):
    return instance.query(
        f"SELECT value FROM system.metrics WHERE metric = '{name}'"
    ).strip()


def wait_metric(instance, name, value):
    wait_query(instance, f"SELECT value FROM system.metrics WHERE metric = '{name}'", value)


def wait_status(instance, status, database="default", view="mv"):
    wait_query(
        instance,
        f"SELECT status FROM system.view_refreshes WHERE database='{database}' AND view='{view}'",
        status,
    )


def create_workload(instance, max_waiting=10):
    instance.query("CREATE RESOURCE query (QUERY)")
    instance.query(
        "CREATE WORKLOAD all SETTINGS max_concurrent_queries=1, "
        f"max_waiting_queries={max_waiting}"
    )


def create_view(instance, name="mv", workload="all"):
    instance.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 APPEND "
        "(workload String, x UInt64) ENGINE Memory EMPTY "
        "AS SELECT getSetting('workload') AS workload, toUInt64(1) AS x "
        f"SETTINGS workload='{workload}'"
    )


@contextmanager
def occupied_slot(instance):
    query_id = f"rmv-slot-blocker-{uuid.uuid4()}"
    errors = []

    def run():
        try:
            # One long-lived query: no release/reacquire gaps while asserting queued state.
            instance.query(
                "SELECT sum(number) FROM numbers(1000000000000) "
                "SETTINGS workload='all', max_threads=1",
                query_id=query_id,
            )
        except Exception as error:
            errors.append(str(error))

    thread = threading.Thread(target=run)
    thread.start()
    try:
        wait_metric(instance, "ConcurrentQueryAcquired", 1)
        assert thread.is_alive(), errors
        yield
    finally:
        instance.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")
        thread.join(timeout=30)
        assert not thread.is_alive()


def test_default_off_preserves_select_workload():
    create_workload(legacy)
    create_view(legacy)
    assert legacy.query(
        "SELECT value FROM system.server_settings "
        "WHERE name='use_query_slot_to_refresh_materialized_view'"
    ).strip() == "0"
    with occupied_slot(legacy):
        legacy.query("SYSTEM REFRESH VIEW mv")
        legacy.query("SYSTEM WAIT VIEW mv", timeout=30)
        assert legacy.query("SELECT workload, x FROM mv") == "all\t1\n"
        assert metric(legacy, "ConcurrentQueryScheduled") == "0"


def test_async_admission_uses_select_workload():
    create_workload(node)
    create_view(node)
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW mv")
        wait_status(node, "WaitingForResource")
        assert metric(node, "ConcurrentQueryScheduled") == "1"
        assert node.query("SELECT count() FROM mv") == "0\n"
        assert node.query(
            "SELECT count() FROM system.background_schedule_pool "
            "WHERE table='mv' AND log_name='RefreshExec' AND executing"
        ) == "0\n"
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT workload, x FROM mv") == "all\t1\n"
    wait_metric(node, "ConcurrentQueryScheduled", 0)
    wait_metric(node, "ConcurrentQueryAcquired", 0)


@pytest.mark.parametrize("query_resource", [False, True])
def test_uncontended_refresh(query_resource):
    if query_resource:
        create_workload(node)
    else:
        node.query("CREATE WORKLOAD all")
    create_view(node)
    node.query("SYSTEM REFRESH VIEW mv")
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT workload, x FROM mv") == "all\t1\n"
    wait_metric(node, "ConcurrentQueryScheduled", 0)
    wait_metric(node, "ConcurrentQueryAcquired", 0)


@pytest.mark.parametrize("operation", ["stop", "drop"])
def test_cancel_queued_admission(operation):
    create_workload(node)
    create_view(node)
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW mv")
        wait_status(node, "WaitingForResource")
        if operation == "stop":
            node.query("SYSTEM STOP VIEW mv", timeout=30)
            wait_status(node, "Disabled")
            assert node.query("SELECT count() FROM mv") == "0\n"
        else:
            node.query("DROP TABLE mv SYNC", timeout=30)
        wait_metric(node, "ConcurrentQueryScheduled", 0)
        assert metric(node, "ConcurrentQueryAcquired") == "1"
    wait_metric(node, "ConcurrentQueryAcquired", 0)
    if operation == "stop":
        node.query("SYSTEM START VIEW mv")
        node.query("SYSTEM REFRESH VIEW mv")
        node.query("SYSTEM WAIT VIEW mv", timeout=30)
        assert node.query("SELECT count() FROM mv") == "1\n"


def test_admission_failure_does_not_execute():
    create_workload(node, max_waiting=1)
    create_view(node, "queued")
    create_view(node)
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW queued")
        wait_status(node, "WaitingForResource", view="queued")
        node.query("SYSTEM REFRESH VIEW mv")
        error = node.query_and_get_error("SYSTEM WAIT VIEW mv", timeout=30)
        assert "max_waiting_queries" in error
        assert node.query("SELECT count() FROM mv") == "0\n"
        node.query("SYSTEM STOP VIEW queued")
        wait_metric(node, "ConcurrentQueryScheduled", 0)
    wait_metric(node, "ConcurrentQueryAcquired", 0)


def test_workload_change_while_queued_requires_new_admission():
    create_workload(node)
    node.query("CREATE WORKLOAD updated IN all")
    create_view(node)
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW mv")
        wait_status(node, "WaitingForResource")
        node.query(
            "ALTER TABLE mv MODIFY QUERY SELECT getSetting('workload') AS workload, "
            "toUInt64(1) AS x SETTINGS workload='updated'"
        )
    error = node.query_and_get_error("SYSTEM WAIT VIEW mv", timeout=30)
    assert "Refresh workload changed" in error
    assert node.query("SELECT count() FROM mv") == "0\n"
    node.query("SYSTEM REFRESH VIEW mv")
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT workload FROM mv") == "updated\n"
    wait_metric(node, "ConcurrentQueryAcquired", 0)


def test_select_change_while_queued_uses_fresh_definition():
    create_workload(node)
    create_view(node)
    with occupied_slot(node):
        node.query("SYSTEM REFRESH VIEW mv")
        wait_status(node, "WaitingForResource")
        node.query(
            "ALTER TABLE mv MODIFY QUERY SELECT getSetting('workload') AS workload, "
            "toUInt64(2) AS x SETTINGS workload='all'"
        )
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT workload, x FROM mv") == "all\t2\n"
    wait_metric(node, "ConcurrentQueryAcquired", 0)


def test_grant_racing_with_stop_releases_slot():
    create_workload(node)
    for _ in range(10):
        create_view(node)
        barrier = threading.Barrier(2)
        errors = []

        def stop():
            try:
                barrier.wait(timeout=30)
                node.query("SYSTEM STOP VIEW mv", timeout=30)
            except Exception as error:
                errors.append(str(error))

        with occupied_slot(node):
            node.query("SYSTEM REFRESH VIEW mv")
            wait_status(node, "WaitingForResource")
            stopper = threading.Thread(target=stop)
            stopper.start()
            barrier.wait(timeout=30)
        stopper.join(timeout=30)
        assert not stopper.is_alive()
        assert not errors
        wait_status(node, "Disabled")
        wait_metric(node, "ConcurrentQueryScheduled", 0)
        wait_metric(node, "ConcurrentQueryAcquired", 0)
        assert int(node.query("SELECT count() FROM mv")) <= 1
        node.query("DROP TABLE mv SYNC")


def test_detach_clears_running_znode_without_session_expiry():
    create_workload(node)
    node.query(
        "CREATE DATABASE rmv_slots "
        "ENGINE=Replicated('/test/rmv_query_slots/database', 's', 'r')"
    )
    create_view(node, "rmv_slots.mv")
    view_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database='rmv_slots' AND name='mv'"
    ).strip()
    keeper = cluster.get_kazoo_client("zoo1")
    path = f"/test/rmv_query_slots/{view_uuid}"
    try:
        with occupied_slot(node):
            node.query("SYSTEM REFRESH VIEW rmv_slots.mv")
            wait_status(node, "WaitingForResource", database="rmv_slots")
            assert keeper.exists(path + "/running") is not None
            node.query("DETACH TABLE rmv_slots.mv PERMANENTLY", timeout=30)
            try:
                assert keeper.exists(path + "/running") is None
                wait_metric(node, "ConcurrentQueryScheduled", 0)
                assert metric(node, "ConcurrentQueryAcquired") == "1"
            finally:
                node.query("ATTACH TABLE rmv_slots.mv")
                node.query("SYSTEM STOP VIEW rmv_slots.mv")
    finally:
        keeper.stop()
        keeper.close()
