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

# The set spills to disk before its first chunk, and every chunk of the subquery becomes a run of the
# external sort. Without compression, the temporary data takes 8 bytes per key in the runs and again
# in the finished set.
SETTINGS = {
    "max_threads": 1,
    "max_block_size": 8192,
    "max_untracked_memory": 0,
    "max_bytes_before_external_set": 1,
    "max_bytes_ratio_before_external_set": 0,
    "temporary_files_codec": "NONE",
    "temporary_files_buffer_size": 65536,
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
    query = f"SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers({rows}))"
    if remote:
        # The remote query fills the set from the subquery of its serialized plan.
        query = f"SELECT count() FROM remote('127.0.0.1', view({query}))"
    return f"{query} FORMAT Null"


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
    # The query that fills the set reports that it spilled to disk, wrote temporary files and read the
    # set from them.
    assert node.query(
        "SELECT spilled_to_disk, ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetWritePart'] > 0, "
        "ProfileEvents['ExternalSetReadBlocks'] > 0 "
        f"FROM system.query_log WHERE initial_query_id = '{query_id}' "
        f"AND type = 'QueryFinish' AND is_initial_query = {int(not remote)}"
    ) == "['set']\t1\t1\t1\n"
    assert_no_temporary_files()


@pytest.mark.parametrize("remote", [False, True], ids=["text", "serialized"])
def test_global_in(remote, user):
    query_id = str(uuid.uuid4())
    settings = {
        **SETTINGS,
        "serialize_query_plan": int(remote),
        "prefer_localhost_replica": 0,
        "max_parallel_replicas": 1,
    }
    # The initiator fills the external table with the result of the subquery, and the remote query fills its
    # set from that table.
    assert node.query(
        "SELECT count() FROM remote('127.0.0.1', numbers(30000)) "
        "WHERE number GLOBAL IN (SELECT number * 3 FROM numbers(16384))",
        settings=settings,
        user=user,
        query_id=query_id,
    ) == "10000\n"
    node.query("SYSTEM FLUSH LOGS query_log")
    assert node.query(
        "SELECT ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetReadBlocks'] > 0 "
        f"FROM system.query_log WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish' AND NOT is_initial_query"
    ) == "1\t1\n"
    assert_no_temporary_files()


def test_concurrent_queries_share_user_limit(user):
    query_id = str(uuid.uuid4())
    failpoint = "infinite_sleep"
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    try:
        # Short-circuit evaluation pauses the subquery only after enough keys have reached the set on disk.
        request = node.get_query_request(
            "SELECT count() FROM numbers(10) WHERE number IN "
            "(SELECT number FROM numbers(65536) WHERE if(number < 49152, 1, sleepEachRow(0) = 0)) FORMAT Null",
            settings={**SETTINGS, "short_circuit_function_evaluation": "force_enable"},
            user=user,
            query_id=query_id,
        )
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        assert node.query(
            "SELECT ProfileEvents['ExternalSetCompressedBytes'] > 300000 "
            f"FROM system.processes WHERE query_id = '{query_id}'"
        ) == "1\n"

        # The set of 49152 keys takes about 800 KB at most, its runs and the finished set together: it fits
        # the quota alone, but not next to the paused query.
        query = spill_query(49152)
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
    node.query(spill_query(49152), settings=SETTINGS, user=user)
