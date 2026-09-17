import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

PAYLOAD_SIZE = 256 * 1024


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        if node.is_built_with_sanitizer():
            pytest.skip("Requires ClickHouse allocation interceptors, which sanitizer builds replace")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
def test_inherited_context_memory_is_recorded_before_protocol_override(batching_limit):
    peak_payload_size = 8 * 1024 * 1024
    session_id = str(uuid.uuid4())
    query_id = str(uuid.uuid4())
    node.http_query(
        None,
        data="SET log_comment = '" + "x" * peak_payload_size + "'",
        params={"session_id": session_id, "max_query_size": 2 * peak_payload_size},
    )

    # The session's large string is copied before the protocol clears its logical value.
    # Its retained capacity must remain included in query memory accounting.
    assert node.http_query(
        "SELECT 1",
        params={
            "session_id": session_id,
            "query_id": query_id,
            "log_comment": "",
            "max_untracked_memory": batching_limit,
            "log_queries": 1,
        },
    ) == "1\n"
    node.query("SYSTEM FLUSH LOGS")
    rows = node.query(
        "SELECT memory_usage, length(log_comment) FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    ).strip().split("\t")
    assert int(rows[0]) >= peak_payload_size
    assert int(rows[1]) == 0

    # The override must not clear the session-owned setting.
    assert node.http_query(
        "SELECT length(getSetting('log_comment'))", params={"session_id": session_id}
    ) == f"{peak_payload_size}\n"
    node.http_query("SELECT 1", params={"session_id": session_id, "close_session": 1})


@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
def test_retained_context_respects_query_limit(batching_limit):
    session_id = str(uuid.uuid4())
    retained_size = 8 * 1024 * 1024
    node.http_query(
        None,
        data="SET log_comment = '" + "x" * retained_size + "'",
        params={"session_id": session_id, "max_query_size": 2 * retained_size},
    )
    try:
        error = node.http_query_and_get_error(
            "SELECT 1",
            params={
                "session_id": session_id,
                "log_comment": "",
                "max_memory_usage": 4 * 1024 * 1024,
                "max_untracked_memory": batching_limit,
            },
        )
        assert "Query memory limit exceeded during query setup" in error
        assert node.http_query("SELECT 1", params={"session_id": session_id}) == "1\n"
    finally:
        node.http_query("SELECT 1", params={"session_id": session_id, "close_session": 1})


@pytest.mark.parametrize("pause_rejection", [False, True])
def test_rejected_admission_does_not_accumulate_user_memory(pause_rejection):
    user = "context_memory_" + uuid.uuid4().hex
    node.query(f"CREATE USER {user}")
    node.query(f"GRANT SELECT ON *.* TO {user}")
    session_id = str(uuid.uuid4())
    # Leave room for the sentinel and ordinary HTTP setup on coverage builds.
    user_limit = 16 * 1024 * 1024
    retained_size = 2 * user_limit
    node.http_query(
        None,
        data="SET log_comment = '" + "x" * retained_size + "'",
        user=user,
        params={
            "session_id": session_id,
            "max_query_size": 2 * retained_size,
            "max_memory_usage_for_user": 0,
            "log_queries": 0,
        },
    )
    sentinel_id = str(uuid.uuid4())
    sentinel = node.get_query_request(
        "SELECT sleepEachRow(1) FROM numbers(600) "
        "SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 10000000000 "
        "FORMAT Null",
        user=user,
        query_id=sentinel_id,
        settings={
            "max_untracked_memory": 0,
            "max_memory_usage_for_user": user_limit,
            "log_queries": 0,
        },
    )
    failpoint = "query_setup_memory_rejection_before_cleanup"
    try:
        assert_eq_with_retry(
            node, f"SELECT count() FROM system.processes WHERE query_id = '{sentinel_id}'", "1"
        )
        assert node.http_query(
            "SELECT 1", user=user, params={"max_memory_usage_for_user": user_limit}
        ) == "1\n"
        before = int(node.query(
            f"SELECT memory_usage FROM system.user_processes WHERE user = '{user}'"
        ))
        for attempt in range(10):
            query_id = str(uuid.uuid4())

            def reject_query():
                return node.http_query_and_get_error(
                    "SELECT 1",
                    user=user,
                    timeout=30,
                    params={
                        "session_id": session_id,
                        "query_id": query_id,
                        "max_memory_usage": 0,
                        "max_memory_usage_for_user": user_limit,
                        "max_untracked_memory": 0,
                        "log_queries": 0,
                    },
                )

            if pause_rejection and attempt == 0:
                node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
                with ThreadPoolExecutor(max_workers=1) as pool:
                    rejected = pool.submit(reject_query)
                    try:
                        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=15)
                        assert node.query(
                            f"SELECT is_cancelled FROM system.processes WHERE query_id = '{query_id}'"
                        ) == "1\n"
                        # Cancellation and inspection can race with rejected-entry cleanup.
                        node.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
                    finally:
                        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
                    error = rejected.result(timeout=30)
                assert node.query(
                    f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
                ) == "0\n"
            else:
                error = reject_query()
            assert "User memory limit exceeded during query setup" in error
        after = int(node.query(
            f"SELECT memory_usage FROM system.user_processes WHERE user = '{user}'"
        ))
        # The sentinel prevents a last-query reset from hiding a credit leaked by rejection.
        assert abs(after - before) < 64 * 1024, (before, after)
        # A fresh context avoids the session's retained string capacity while checking
        # that the same user can still admit a small query with the sentinel active.
        assert node.http_query(
            "SELECT 1",
            user=user,
            params={
                "max_memory_usage_for_user": user_limit,
            },
        ) == "1\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node.query(f"KILL QUERY WHERE query_id = '{sentinel_id}' SYNC")
        sentinel.get_answer_and_error()
        node.http_query(
            "SELECT 1",
            user=user,
            params={"session_id": session_id, "log_comment": "", "close_session": 1},
        )
        node.query(f"DROP USER {user}")


@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
def test_synchronous_queries_do_not_accumulate_setup_memory(batching_limit):
    user = "context_memory_" + uuid.uuid4().hex
    session_id = str(uuid.uuid4())
    sentinel_id = str(uuid.uuid4())
    node.query(f"CREATE USER {user}")
    node.query(f"GRANT SELECT ON *.* TO {user}")
    node.http_query(
        None,
        data="SET log_comment = '" + "x" * PAYLOAD_SIZE + "'",
        user=user,
        params={"session_id": session_id, "max_query_size": 2 * PAYLOAD_SIZE},
    )
    sentinel = node.get_query_request(
        "SELECT repeat('ssssssssssssssssssssssssssssssss', 524288), sleep(600) "
        "SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 10000000000 "
        "FORMAT Null",
        user=user,
        query_id=sentinel_id,
        settings={"max_untracked_memory": 0, "log_queries": 0},
    )
    try:
        assert_eq_with_retry(
            node, f"SELECT count() FROM system.processes WHERE query_id = '{sentinel_id}'", "1"
        )

        # `SET` completes on the request thread, isolating setup accounting from
        # existing executor allocations freed by a different thread after detachment.
        query = "SET max_block_size = 65536 /*" + "q" * 8192 + "*/"

        def small_query():
            assert node.http_query(
                query,
                user=user,
                params={
                    "session_id": session_id,
                    "log_comment": "",
                    "log_queries": 0,
                    "max_untracked_memory": batching_limit,
                },
            ) == ""

        for _ in range(32):
            small_query()

        def setup_balance():
            row = node.query(
                "SELECT memory_usage, "
                "(SELECT memory_usage FROM system.processes "
                f"WHERE query_id = '{sentinel_id}') "
                f"FROM system.user_processes WHERE user = '{user}'"
            ).strip().split("\t")
            user_memory, sentinel_memory = map(int, row)
            # A positive floor prevents saturation from hiding excess debits.
            assert user_memory >= 16 * 1024 * 1024, row
            assert sentinel_memory >= 16 * 1024 * 1024, row
            return user_memory - sentinel_memory

        before = setup_balance()
        samples = []
        for _ in range(3):
            for _ in range(128):
                small_query()
            samples.append(setup_balance())
        # A live query prevents resets, and enough repetitions expose even a small
        # per-query residue from guards, group metadata, or weak-reference storage.
        assert node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{sentinel_id}'"
        ) == "1\n"
        assert max(abs(sample - before) for sample in samples) < 2048, (before, samples)
    finally:
        node.query(f"KILL QUERY WHERE query_id = '{sentinel_id}' SYNC")
        sentinel.get_answer_and_error()
        node.http_query(
            "SELECT 1",
            user=user,
            params={"session_id": session_id, "log_comment": "", "close_session": 1},
        )
        node.query(f"DROP USER {user}")
