import threading
import time
import uuid
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/query_slots.xml"],
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)
legacy = cluster.add_instance(
    "legacy",
    with_zookeeper=True,
    stay_alive=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)


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
        instance.query("DROP WORKLOAD IF EXISTS original")
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


def create_view(instance, name="mv", workload="all", setting="refresh_workload"):
    instance.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 APPEND "
        "(workload String, x UInt64) ENGINE Memory EMPTY "
        "AS SELECT getSetting('workload') AS workload, toUInt64(1) AS x "
        f"SETTINGS {setting}='{workload}'",
        timeout=30,
    )


@contextmanager
def occupied_slot(instance, workload="all"):
    query_id = f"rmv-slot-blocker-{uuid.uuid4()}"
    errors = []

    def run():
        try:
            # One long-lived query: no release/reacquire gaps while asserting queued state.
            instance.query(
                "SELECT sum(number) FROM numbers(1000000000000) "
                f"SETTINGS workload='{workload}', max_threads=1",
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
    create_view(legacy, setting="workload")
    assert legacy.query(
        "SELECT value FROM system.server_settings "
        "WHERE name='use_query_slot_to_refresh_materialized_view'"
    ).strip() == "0"
    with occupied_slot(legacy):
        legacy.query("SYSTEM REFRESH VIEW mv")
        legacy.query("SYSTEM WAIT VIEW mv", timeout=30)
        assert legacy.query("SELECT workload, x FROM mv") == "all\t1\n"
        assert metric(legacy, "ConcurrentQueryScheduled") == "0"


def test_refresh_workload_with_admission_disabled():
    create_workload(legacy)
    with occupied_slot(legacy):
        assert legacy.query(
            "SELECT getSetting('workload') SETTINGS refresh_workload='all'", timeout=10
        ) == "default\n"
        create_view(legacy)
        legacy.query("SYSTEM REFRESH VIEW mv")
        legacy.query("SYSTEM WAIT VIEW mv", timeout=30)
        assert legacy.query("SELECT workload FROM mv") == "all\n"
        assert metric(legacy, "ConcurrentQueryScheduled") == "0"


@pytest.mark.parametrize("replicated", [False, True])
def test_refresh_workload_separates_create_from_refresh(replicated):
    node.query(
        "CREATE RESOURCE query (QUERY);"
        "CREATE WORKLOAD all;"
        "CREATE WORKLOAD original IN all;"
        "CREATE WORKLOAD updated IN all SETTINGS max_concurrent_queries=1"
    )
    engine = (
        "Replicated('/test/rmv_query_slots/create', 's', 'r')"
        if replicated else "Atomic"
    )
    node.query(f"CREATE DATABASE rmv_slots ENGINE={engine}", timeout=30)
    with occupied_slot(node, workload="updated"):
        # Both the outer CREATE and its replicated DDL execution use original. The only
        # runtime slot is occupied, but creating the view must still complete.
        node.query(
            "CREATE MATERIALIZED VIEW rmv_slots.mv REFRESH EVERY 1 YEAR "
            "SETTINGS refresh_retries=0 APPEND (workload String, x UInt64) ENGINE Memory EMPTY "
            "AS SELECT workload, toUInt64(1) AS x FROM "
            "(SELECT getSetting('workload') AS workload SETTINGS workload='original') "
            "SETTINGS workload='original', refresh_workload='updated'",
            timeout=30,
        )
        definition = node.query("SHOW CREATE TABLE rmv_slots.mv")
        wait_metric(node, "ConcurrentQueryAcquired", 1)
        assert metric(node, "ConcurrentQueryScheduled") == "0"
        node.query("SYSTEM REFRESH VIEW rmv_slots.mv")
        wait_status(node, "WaitingForResource", database="rmv_slots")
        assert node.query("SELECT count() FROM rmv_slots.mv") == "0\n"
    node.query("SYSTEM WAIT VIEW rmv_slots.mv", timeout=30)
    # Reapplying outer or nested SELECT settings must not restore the CREATE workload.
    assert node.query("SELECT workload, x FROM rmv_slots.mv") == "updated\t1\n"
    assert node.query("SHOW CREATE TABLE rmv_slots.mv") == definition
    wait_metric(node, "ConcurrentQueryAcquired", 0)


@pytest.mark.parametrize("setting", ["workload", "refresh_workload"])
def test_async_admission_uses_select_workload(setting):
    create_workload(node)
    create_view(node, setting=setting)
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


@pytest.mark.parametrize("pause_query", ["SYSTEM PAUSE VIEW mv", "SYSTEM PAUSE VIEWS"])
def test_pause_cancels_queued_admission(pause_query):
    create_workload(node)
    create_view(node)
    try:
        with occupied_slot(node):
            node.query("SYSTEM REFRESH VIEW mv")
            wait_status(node, "WaitingForResource")
            node.query(pause_query, timeout=30)
            wait_status(node, "Disabled")
            wait_metric(node, "ConcurrentQueryScheduled", 0)
            assert metric(node, "ConcurrentQueryAcquired") == "1"
            assert node.query("SELECT count() FROM mv") == "0\n"
        wait_metric(node, "ConcurrentQueryAcquired", 0)
        assert node.query("SELECT count() FROM mv") == "0\n"
        wait_status(node, "Disabled")
        node.query("SYSTEM START VIEW mv")
        node.query("SYSTEM REFRESH VIEW mv")
        node.query("SYSTEM WAIT VIEW mv", timeout=30)
        assert node.query("SELECT count() FROM mv") == "1\n"
    finally:
        # Clear any pause locks before the next test, including after an assertion failure.
        node.query("SYSTEM START VIEWS")


def test_pause_allows_running_refresh_to_finish():
    create_workload(node)
    node.query(
        "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 APPEND (x UInt64) ENGINE Memory EMPTY "
        "AS SELECT sum(sleepEachRow(1)) AS x FROM numbers(10) "
        "SETTINGS refresh_workload='all', max_threads=1, max_block_size=1"
    )
    node.query("SYSTEM REFRESH VIEW mv")
    # Running also covers the short dispatch window. The process-list entry proves that
    # admission has been consumed and the refresh execution has actually started.
    wait_query(
        node,
        "SELECT count() FROM system.processes "
        "WHERE Settings['log_comment']='refresh of default.mv' AND is_internal",
        1,
    )
    node.query("SYSTEM PAUSE VIEW mv")
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT count() FROM mv") == "1\n"
    wait_status(node, "Disabled")
    wait_metric(node, "ConcurrentQueryAcquired", 0)


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
    node.query("CREATE WORKLOAD original IN all")
    node.query("CREATE WORKLOAD updated IN all")
    create_view(node, workload="original")
    with occupied_slot(node, workload="original"):
        node.query("SYSTEM REFRESH VIEW mv")
        wait_status(node, "WaitingForResource")
        node.query(
            "ALTER TABLE mv MODIFY QUERY SELECT getSetting('workload') AS workload, "
            "toUInt64(1) AS x SETTINGS refresh_workload='updated'"
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
            "toUInt64(2) AS x SETTINGS refresh_workload='all'"
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
    keeper = cluster.get_kazoo_client("zoo1")
    try:
        with occupied_slot(node):
            # Replicated CREATE must not request a slot from the full runtime workload.
            create_view(node, "rmv_slots.mv")
            view_uuid = node.query(
                "SELECT uuid FROM system.tables WHERE database='rmv_slots' AND name='mv'"
            ).strip()
            path = f"/test/rmv_query_slots/{view_uuid}"
            node.query("SYSTEM REFRESH VIEW rmv_slots.mv")
            wait_status(node, "WaitingForResource", database="rmv_slots")
            assert keeper.exists(path + "/running") is not None
            node.query("DETACH TABLE rmv_slots.mv PERMANENTLY SYNC", timeout=30)
            try:
                assert keeper.exists(path + "/running") is None
                assert b"refresh_running: 0" in keeper.get(path)[0]
                wait_metric(node, "ConcurrentQueryScheduled", 0)
                assert metric(node, "ConcurrentQueryAcquired") == "1"
            finally:
                node.query("ATTACH TABLE rmv_slots.mv")
                node.query("SYSTEM STOP VIEW rmv_slots.mv")
    finally:
        keeper.stop()
        keeper.close()


@pytest.mark.parametrize("operation", ["STOP", "PAUSE"])
def test_cancel_granted_admission_before_execution(operation):
    create_workload(node)
    create_view(node)
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_skip_execution")
    try:
        node.query("SYSTEM REFRESH VIEW mv")
        # The failpoint keeps the attempt in Requested without occupying a pool worker.
        wait_status(node, "Running")
        wait_metric(node, "ConcurrentQueryAcquired", 1)
        node.query(f"SYSTEM {operation} VIEW mv", timeout=30)
        # Check while execution is still suppressed: it cannot release the slot for us.
        wait_metric(node, "ConcurrentQueryAcquired", 0)
        assert metric(node, "ConcurrentQueryScheduled") == "0"
        assert node.query("SELECT count() FROM mv") == "0\n"
        assert node.query("SELECT 1 SETTINGS workload='all'", timeout=10) == "1\n"
    finally:
        # Drop while execution is suppressed so shutdown reconciles the Requested attempt.
        try:
            node.query("DROP TABLE mv SYNC", timeout=30)
        finally:
            node.query("SYSTEM DISABLE FAILPOINT refresh_mv_skip_execution")


def test_query_slot_released_before_exchange():
    create_workload(node)
    node.query(
        "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR "
        "SETTINGS refresh_retries=0 (x UInt64) ENGINE Memory EMPTY "
        "AS SELECT toUInt64(1) AS x SETTINGS refresh_workload='all'"
    )
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_pause_before_exchange")
    try:
        node.query("SYSTEM REFRESH VIEW mv")
        node.query(
            "SYSTEM WAIT FAILPOINT refresh_mv_pause_before_exchange PAUSE", timeout=30
        )
        # INSERT SELECT is finished, but the replacement has not been published yet.
        assert node.query("SELECT count() FROM mv") == "0\n"
        wait_metric(node, "ConcurrentQueryAcquired", 0)
        assert node.query("SELECT 1 SETTINGS workload='all'", timeout=10) == "1\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT refresh_mv_pause_before_exchange")
    node.query("SYSTEM WAIT VIEW mv", timeout=30)
    assert node.query("SELECT x FROM mv") == "1\n"
    wait_metric(node, "ConcurrentQueryAcquired", 0)
