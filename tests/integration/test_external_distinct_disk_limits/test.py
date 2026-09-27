import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/limits.yaml"],
    user_configs=["configs/users.yaml"],
)

SETTINGS = {
    "max_threads": 1,
    "max_block_size": 8192,
    "max_untracked_memory": 0,
    "max_bytes_before_external_distinct": 1,
    "max_bytes_ratio_before_external_distinct": 0,
    "temporary_files_codec": "NONE",
    "temporary_files_buffer_size": 65536,
    "optimize_distinct_in_order": 0,
    "log_queries": 1,
}


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture
def user():
    name = f"disk_quota_{uuid.uuid4().hex}"
    node.query(
        f"CREATE USER {name}; GRANT SELECT, REMOTE, CREATE TEMPORARY TABLE ON *.* TO {name}"
    )
    try:
        yield name
    finally:
        node.query(f"DROP USER {name}")


def spill_query(rows, remote=False):
    source = f"numbers({rows})"
    if remote:
        source = f"remote('127.0.0.1', view(SELECT number FROM {source}))"
    return f"SELECT DISTINCT number FROM {source} FORMAT Null"


def assert_no_temporary_files():
    wait_condition(
        lambda: node.exec_in_container(["ls", "-1", "/var/lib/clickhouse/tmp/"]),
        lambda files: files == "",
        max_attempts=100,
        delay=0.1,
    )


@pytest.mark.parametrize("remote", [False, True], ids=["local", "serialized"])
def test_query_limit(remote, user):
    settings = {
        **SETTINGS,
        "max_temporary_data_on_disk_size_for_query": 65536,
        "serialize_query_plan": int(remote),
        "prefer_localhost_replica": 0,
        "max_parallel_replicas": 1,
    }
    error = node.query_and_get_error(
        spill_query(262144, remote), settings=settings, user=user
    )
    assert "Limit for temporary files size exceeded" in error
    assert "/ 65536 bytes" in error
    assert_no_temporary_files()
    node.query(spill_query(16384), settings=SETTINGS, user=user)


def test_user_limit(user):
    error = node.query_and_get_error(spill_query(262144), settings=SETTINGS, user=user)
    assert "Limit for temporary files size exceeded" in error
    assert "/ 1048576 bytes" in error
    assert_no_temporary_files()
    node.query(spill_query(16384), settings=SETTINGS, user=user)


@pytest.mark.parametrize("remote", [False, True], ids=["local", "serialized"])
def test_spill_below_limits(remote, user):
    query_id = str(uuid.uuid4())
    settings = {
        **SETTINGS,
        "max_temporary_data_on_disk_size_for_query": 524288,
        "serialize_query_plan": int(remote),
        "prefer_localhost_replica": 0,
        "max_parallel_replicas": 1,
    }
    node.query(
        spill_query(16384, remote),
        settings=settings,
        user=user,
        query_id=query_id,
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    assert node.query(
        f"SELECT count() FROM system.query_log WHERE initial_query_id = '{query_id}' "
        f"AND type = 'QueryFinish' AND is_initial_query = {int(not remote)} "
        "AND ProfileEvents['ExternalDistinctWritePart'] > 0"
    ) == "1\n"
    assert_no_temporary_files()


def test_concurrent_queries_share_user_limit(user):
    query_id = str(uuid.uuid4())
    failpoint = "infinite_sleep"
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    try:
        # Short-circuit evaluation pauses the input only after enough rows have reached `DISTINCT`.
        request = node.get_query_request(
            "SELECT DISTINCT number FROM numbers(65536) "
            "WHERE if(number < 49152, 1, sleepEachRow(0) = 0) FORMAT Null",
            settings={**SETTINGS, "short_circuit_function_evaluation": "force_enable"},
            user=user,
            query_id=query_id,
        )
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        assert node.query(
            "SELECT ProfileEvents['ExternalDistinctCompressedBytes'] > 400000 "
            f"FROM system.processes WHERE query_id = '{query_id}'"
        ) == "1\n"

        query = spill_query(81920)
        # Another user has an independent quota, even while the first query's files remain live.
        node.query(query, settings=SETTINGS)
        error = node.query_and_get_error(query, settings=SETTINGS, user=user)
        assert "Limit for temporary files size exceeded" in error
        assert "/ 1048576 bytes" in error
    finally:
        try:
            node.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

    assert "QUERY_WAS_CANCELLED" in request.get_error()
    assert_eq_with_retry(
        node, f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'", "0"
    )
    assert_no_temporary_files()
    node.query(spill_query(81920), settings=SETTINGS, user=user)
