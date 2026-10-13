import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
    with_zookeeper=True,
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
    with_remote_database_disk=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(table, engine, check_projection=False, storage_policy="s3"):
    projection = ", PROJECTION p (SELECT n ORDER BY n)" if check_projection else ""
    if engine == "MergeTree":
        engine_clause = "MergeTree"
    else:
        engine_clause = f"ReplicatedMergeTree('/check_cancellation/{table}', 'r1')"

    node.query(
        f"CREATE TABLE {table} (n UInt64{projection}) ENGINE={engine_clause} ORDER BY n "
        f"SETTINGS storage_policy='{storage_policy}', min_bytes_for_wide_part=0, "
        "prewarm_mark_cache=0, prewarm_primary_key_cache=0"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    if engine != "MergeTree":
        node.query(f"SYSTEM STOP FETCHES {table}")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(4096)")


def table_state(table):
    active_parts = node.query(
        "SELECT name FROM system.parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active ORDER BY name"
    )
    detached_parts = node.query(
        "SELECT name, reason FROM system.detached_parts "
        f"WHERE database=currentDatabase() AND table='{table}' ORDER BY name, reason"
    )
    fetches = node.query(
        "SELECT count() FROM system.replication_queue "
        f"WHERE database=currentDatabase() AND table='{table}' AND type='GET_PART'"
    ).strip()
    return active_parts, detached_parts, fetches


def projection_state(table):
    return node.query(
        "SELECT is_broken, exception_code, exception FROM system.projection_parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active ORDER BY name"
    )


def cancel_check(table, cancel_method, single_value, check_projection=False):
    query_id = uuid.uuid4().hex
    pause_failpoint = (
        "check_data_part_before_projection_read"
        if check_projection
        else "s3_read_before_get_object"
    )
    injected_failpoint = "query_status_cancel_with_injected_exception"
    enabled_failpoints = []
    request = None
    request_finished = False

    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {pause_failpoint}")
        enabled_failpoints.append(pause_failpoint)
        if cancel_method == "injected_exception":
            node.query(f"SYSTEM ENABLE FAILPOINT {injected_failpoint}")
            enabled_failpoints.append(injected_failpoint)

        timeout_settings = (
            ", max_execution_time=1, timeout_overflow_mode='throw'"
            if cancel_method == "timeout"
            else ""
        )
        request = node.get_query_request(
            f"CHECK TABLE {table} SETTINGS max_threads=1, "
            f"check_query_single_value_result={single_value}, enable_filesystem_cache=0"
            f"{timeout_settings}",
            query_id=query_id,
            timeout=180,
        )
        node.query(f"SYSTEM WAIT FAILPOINT {pause_failpoint} PAUSE", timeout=60)

        if cancel_method != "timeout":
            node.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
        assert_eq_with_retry(
            node,
            f"SELECT is_cancelled FROM system.processes WHERE query_id='{query_id}'",
            "1",
            retry_count=40,
            sleep_time=0.25,
        )

        node.query(f"SYSTEM NOTIFY FAILPOINT {pause_failpoint}")
        answer, error = request.get_answer_and_error()
        request_finished = True
        assert answer == "", answer
        expected_error = {
            "timeout": "TIMEOUT_EXCEEDED",
            "kill_query": "QUERY_WAS_CANCELLED",
            "injected_exception": "FAULT_INJECTED",
        }[cancel_method]
        assert expected_error in error, error
        if cancel_method == "injected_exception":
            assert "Injected query cancellation exception" in error, error
    finally:
        if pause_failpoint in enabled_failpoints:
            node.query(f"SYSTEM NOTIFY FAILPOINT {pause_failpoint}")
        for failpoint in reversed(enabled_failpoints):
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        if request is not None and not request_finished:
            request.get_answer_and_error()


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
@pytest.mark.parametrize("check_projection", [False, True], ids=["part", "projection"])
@pytest.mark.parametrize("cancel_method", ["timeout", "kill_query"])
@pytest.mark.parametrize("single_value", [0, 1], ids=["rows", "single"])
def test_check_cancellation_preserves_part(
    started_cluster, engine, check_projection, cancel_method, single_value
):
    table = f"check_cancel_{uuid.uuid4().hex}"
    create_table(table, engine, check_projection)
    state_before = table_state(table)
    projection_before = projection_state(table) if check_projection else None

    try:
        assert node.query(f"SELECT count(), sum(n) FROM {table}").strip() == "4096\t8386560"
        cancel_check(table, cancel_method, single_value, check_projection)

        assert table_state(table) == state_before
        assert node.query(f"SELECT count(), sum(n) FROM {table}").strip() == "4096\t8386560"
        if check_projection:
            assert projection_state(table) == projection_before
            assert projection_before.startswith("0\t0\t")

        assert node.query(
            f"CHECK TABLE {table} SETTINGS check_query_single_value_result=1"
        ) == "1\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


def test_stored_cancellation_exception_preserves_projection(started_cluster):
    table = f"check_cancel_exception_{uuid.uuid4().hex}"
    create_table(table, "ReplicatedMergeTree", check_projection=True)
    state_before = table_state(table)
    projection_before = projection_state(table)

    try:
        cancel_check(table, "injected_exception", 0, check_projection=True)
        assert table_state(table) == state_before
        assert projection_state(table) == projection_before
        assert node.query(f"SELECT count(), sum(n) FROM {table}").strip() == "4096\t8386560"
        assert node.query(
            f"CHECK TABLE {table} SETTINGS check_query_single_value_result=1"
        ) == "1\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("cancel_method", ["timeout", "kill_query"])
def test_check_cancellation_preserves_filesystem_cache(started_cluster, cancel_method):
    table = f"check_cancel_cache_{uuid.uuid4().hex}"
    create_table(table, "MergeTree", storage_policy="s3_cache")

    try:
        node.query("SYSTEM DROP FILESYSTEM CACHE")
        assert node.query(
            f"SELECT sum(n) FROM {table} SETTINGS enable_filesystem_cache=1"
        ) == "8386560\n"
        cache_before = node.query(
            "SELECT key, file_segment_range_begin, size, state FROM system.filesystem_cache "
            "WHERE cache_name='s3_cache' ORDER BY key, file_segment_range_begin, size, state"
        )
        assert cache_before

        cancel_check(table, cancel_method, 0)

        cache_after = node.query(
            "SELECT key, file_segment_range_begin, size, state FROM system.filesystem_cache "
            "WHERE cache_name='s3_cache' ORDER BY key, file_segment_range_begin, size, state"
        )
        assert cache_after == cache_before
        assert node.query(
            f"SELECT sum(n) FROM {table} SETTINGS enable_filesystem_cache=1"
        ) == "8386560\n"
        assert node.query(
            f"CHECK TABLE {table} SETTINGS check_query_single_value_result=1"
        ) == "1\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")
