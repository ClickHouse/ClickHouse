import os
import sys
import threading
import time

import grpc
import psycopg2 as py_psql
import pymysql.connections
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt

script_dir = os.path.dirname(os.path.realpath(__file__))
grpc_protocol_pb2_dir = os.path.join(script_dir, "grpc_protocol_pb2")
if grpc_protocol_pb2_dir not in sys.path:
    sys.path.append(grpc_protocol_pb2_dir)
import clickhouse_grpc_pb2  # Execute grpc_protocol_pb2/generate.py to generate these modules.
import clickhouse_grpc_pb2_grpc

MAX_SESSIONS_FOR_USER = 2
SESSIONS_ESTABLISHED_TIMEOUT = 180
# The refusal arrives while the session is created, so the probe below cannot need more
# time than establishing a session does.
OVER_LIMIT_TIMEOUT = SESSIONS_ESTABLISHED_TIMEOUT
# Per query for this test's own bookkeeping. Without it the client helper waits
# DEFAULT_QUERY_TIMEOUT = 600s, which would make the budgets above advisory.
CONTROL_QUERY_TIMEOUT = 30
OVER_LIMIT_QUERY = "SELECT 1"
SESSION_REFUSED_ERROR = "overflown session count"
POSTGRES_SERVER_PORT = 5433
MYSQL_SERVER_PORT = 9001
GRPC_PORT = 9100

TEST_USER = "test_user"
TEST_PASSWORD = "123"

DEFAULT_ENCODING = "utf-8"


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ports.xml",
        "configs/log.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=["configs/users.xml"],
    env_variables={
        # Bug in TSAN reproduces in this test https://github.com/grpc/grpc/issues/29550#issuecomment-1188085387
        "TSAN_OPTIONS": "report_atomic_races=0 "
        + os.getenv("TSAN_OPTIONS", default=""),
    },
)


def get_query(name, id):
    return f"SELECT '{name}', {id}, COUNT(*) from system.numbers"


def grpc_get_url():
    return f"{instance.ip_address}:{GRPC_PORT}"


def grpc_create_insecure_channel():
    channel = grpc.insecure_channel(grpc_get_url())
    grpc.channel_ready_future(channel).result(timeout=2)
    return channel


def grpc_query(query_text, channel, session_id_, timeout=None):
    query_info = clickhouse_grpc_pb2.QueryInfo(
        query=query_text,
        session_id=session_id_,
        user_name=TEST_USER,
        password=TEST_PASSWORD,
    )

    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    result = stub.ExecuteQuery(query_info, timeout=timeout)
    if result and result.HasField("exception"):
        raise Exception(result.exception.display_text)
    return result.output.decode(DEFAULT_ENCODING)


def wait_for_user_queries(count, threads, deadline):
    # A running query implies a tracked session for that user, because the session is
    # created before the query. The converse does not hold, so this can prove sessions
    # present, never absent.
    started = time.monotonic()
    running = 0
    while True:
        # A whole CONTROL_QUERY_TIMEOUT per poll, not the rest of the budget: a poll cut
        # short by the deadline would report the client instead of the sessions. The barrier
        # can therefore overrun SESSIONS_ESTABLISHED_TIMEOUT by up to one poll.
        running = int(
            instance.query(
                f"SELECT count() FROM system.processes WHERE user = '{TEST_USER}'",
                timeout=CONTROL_QUERY_TIMEOUT,
            ).strip()
        )
        if running >= count:
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(0.5)

    errors = [thread.exception for thread in threads if thread.exception]
    pytest.fail(
        f"{running} of {count} sessions for {TEST_USER} were established in "
        f"{time.monotonic() - started:.0f}s; session errors: {errors}"
    )


def reclaim_sessions(thread_list):
    # A single KILL snapshots system.processes once. An accepted session whose
    # query has not registered yet (slow under sanitizers) survives and its
    # thread blocks forever. Re-issue KILL until every worker thread has exited.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        instance.query(
            f"KILL QUERY WHERE user='{TEST_USER}' SYNC",
            timeout=CONTROL_QUERY_TIMEOUT,
        )
        for thread in thread_list:
            thread.join(timeout=1)
        if not any(thread.is_alive() for thread in thread_list):
            return

    # Bounded: never fall through to an unbounded join (would hang to the pytest timeout).
    if any(thread.is_alive() for thread in thread_list):
        pytest.fail("Timed out waiting for session threads to finish after KILL QUERY")


def occupy_every_session(deadline):
    # A client that has finished its query still holds a tracked session, so this
    # saturates the limit while leaving `system.processes` empty.
    while True:
        conns = []
        try:
            for _ in range(MAX_SESSIONS_FOR_USER):
                conns.append(
                    pymysql.connections.Connection(
                        host=instance.ip_address,
                        user=TEST_USER,
                        password=TEST_PASSWORD,
                        database="default",
                        port=MYSQL_SERVER_PORT,
                        read_timeout=CONTROL_QUERY_TIMEOUT,
                        write_timeout=CONTROL_QUERY_TIMEOUT,
                    )
                )
                conns[-1].cursor().execute("SELECT 1")
            return conns
        except Exception as ex:
            for conn in conns:
                conn.close()
            # A refusal here is the previous call's sessions still being tracked, the
            # transient state SessionHolder retries through.
            if SESSION_REFUSED_ERROR not in str(ex) or time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


def threaded_run_test(sessions):
    holders = sessions[:MAX_SESSIONS_FOR_USER]
    over_limit = sessions[MAX_SESSIONS_FOR_USER:]

    # One deadline for the whole establishment phase, so a holder stops retrying exactly
    # when the barrier stops waiting for it.
    deadline = time.monotonic() + SESSIONS_ESTABLISHED_TIMEOUT
    thread_list = [
        SessionHolder(session, i, deadline) for i, session in enumerate(holders)
    ]
    for thread in thread_list:
        thread.start()

    try:
        wait_for_user_queries(len(holders), thread_list, deadline)

        # Not a thread: `join(timeout=...)` returns while a blocked client call keeps
        # running into the next test, holding its session. The deadline goes to the client
        # instead; libpq's bounds the connection phase only, which is where a refusal
        # arrives (`ErrorResponse` precedes `ReadyForQuery`).
        for i, session in enumerate(over_limit, start=len(holders)):
            try:
                session(i, OVER_LIMIT_QUERY, OVER_LIMIT_TIMEOUT)
            except Exception as ex:
                assert SESSION_REFUSED_ERROR in str(
                    ex
                ), f"session {i} over the limit failed for another reason: {ex!r}"
            else:
                pytest.fail(f"session {i} over the limit was not refused")
    finally:
        reclaim_sessions(thread_list)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Wait for the PostgreSQL handler to start.
        # Cluster.start waits until port 9000 becomes accessible.
        # Server opens the PostgreSQL compatibility port a bit later.
        instance.wait_for_log_line("PostgreSQL compatibility protocol")
        yield cluster
    finally:
        cluster.shutdown()


class SessionHolder(threading.Thread):
    # A class attribute so the main thread can read it before `run` starts, and text
    # rather than the exception object: that would retain its traceback, and with it the
    # frame owning the protocol client, so the session would outlive the thread and be
    # counted against the next test.
    exception = None

    def __init__(self, session, id, deadline):
        super().__init__()
        self.session = session
        self.id = id
        self.deadline = deadline

    def run(self):
        while True:
            try:
                self.session(self.id)
                return
            except Exception as ex:
                self.exception = f"{type(ex).__name__}: {ex}"
                # A refused holder created no session, so the limit is saturated by
                # sessions this call did not open: a slot is freed when the server
                # destroys the connection, which the previous call's KILL QUERY does not
                # wait for. That is transient, so keep trying for the whole deadline.
                if SESSION_REFUSED_ERROR not in str(ex):
                    return
                if time.monotonic() >= self.deadline:
                    return
                time.sleep(0.5)


def postgres_session(id, query=None, timeout=None):
    ch = py_psql.connect(
        host=instance.ip_address,
        port=POSTGRES_SERVER_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database="default",
        # libpq bounds the whole connection phase, which is where the refusal arrives.
        connect_timeout=timeout,
    )
    cur = ch.cursor()
    cur.execute(query or get_query("postgres_session", id))
    cur.fetchall()


def mysql_session(id, query=None, timeout=None):
    client = pymysql.connections.Connection(
        host=instance.ip_address,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database="default",
        port=MYSQL_SERVER_PORT,
        # Not `connect_timeout`: pymysql clears the socket timeout before the handshake,
        # and the refusal arrives in the handshake.
        read_timeout=timeout,
        write_timeout=timeout,
    )
    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query or get_query("mysql_session", id))
    cursor.fetchall()


def tcp_session(id, query=None, timeout=None):
    instance.query(
        query or get_query("tcp_session", id),
        user=TEST_USER,
        password=TEST_PASSWORD,
        timeout=timeout,
    )


def http_session(id, query=None, timeout=None):
    instance.http_query(
        query or get_query("http_session", id),
        user=TEST_USER,
        password=TEST_PASSWORD,
        timeout=timeout,
    )


def http_named_session(id, query=None, timeout=None):
    instance.http_query(
        query or get_query("http_named_session", id),
        user=TEST_USER,
        password=TEST_PASSWORD,
        params={"session_id": id},
        timeout=timeout,
    )


def grpc_session(id, query=None, timeout=None):
    grpc_query(
        query or get_query("grpc_session", id),
        grpc_create_insecure_channel(),
        f"session_{id}",
        timeout=timeout,
    )


def test_profile_max_sessions_for_user_tcp(started_cluster):
    threaded_run_test([tcp_session] * 3)


def test_profile_max_sessions_for_user_postgres(started_cluster):
    threaded_run_test([postgres_session] * 3)


def test_profile_max_sessions_for_user_mysql(started_cluster):
    threaded_run_test([mysql_session] * 3)


def test_profile_max_sessions_for_user_http(started_cluster):
    threaded_run_test([http_session] * 3)


def test_profile_max_sessions_for_user_http_named_session(started_cluster):
    threaded_run_test([http_named_session] * 3)


def test_profile_max_sessions_for_user_grpc(started_cluster):
    threaded_run_test([grpc_session] * 3)


def test_profile_max_sessions_for_user_tcp_and_others(started_cluster):
    threaded_run_test([tcp_session, grpc_session, grpc_session])
    threaded_run_test([tcp_session, http_session, http_session])
    threaded_run_test([tcp_session, mysql_session, mysql_session])
    threaded_run_test([tcp_session, postgres_session, postgres_session])
    threaded_run_test([tcp_session, http_session, postgres_session])
    threaded_run_test([tcp_session, postgres_session, http_session])


def test_profile_max_sessions_for_user_holder_retries_after_refusal(started_cluster):
    deadline = time.monotonic() + SESSIONS_ESTABLISHED_TIMEOUT
    occupied = occupy_every_session(deadline)
    holder = SessionHolder(tcp_session, 0, deadline)
    holder.start()
    try:
        # Free the slots only once the holder has actually been refused, so the holder
        # has to retry to reach the barrier. Waiting for a duration instead would let
        # this pass with no retry at all.
        while not holder.exception:
            assert holder.is_alive(), "the holder ended without being refused"
            assert time.monotonic() < deadline, "the holder was never refused"
            time.sleep(0.1)
        assert SESSION_REFUSED_ERROR in holder.exception, holder.exception

        for conn in occupied:
            conn.close()
        occupied = []
        wait_for_user_queries(1, [holder], deadline)
    finally:
        for conn in occupied:
            conn.close()
        reclaim_sessions([holder])


def test_profile_max_sessions_for_user_setting_in_query(started_cluster):
    instance.query_and_get_error("SET max_sessions_for_user = 10")


def test_profile_max_sessions_for_user_client_suggestions_connection(started_cluster):
    command_text = f"{started_cluster.get_client_cmd()} --host {instance.ip_address} --port 9000 -u {TEST_USER} --password {TEST_PASSWORD}"
    command_text_without_suggestions = command_text + " --disable_suggestion"

    # Launch client1 without suggestions to avoid a race condition:
    # Client1 opens a session.
    # Client1 opens a session for suggestion connection.
    # Client2 fails to open a session and gets the USER_SESSION_LIMIT_EXCEEDED error.
    #
    # Expected order:
    # Client1 opens a session.
    # Client2 opens a session.
    # Client2 fails to open a session for suggestions and with USER_SESSION_LIMIT_EXCEEDED (No error printed).
    # Client3 fails to open a session.
    # Client1 executes the query.
    # Client2 loads suggestions from the server using the main connection and executes a query.
    with client(
        name="client1>", log=None, command=command_text_without_suggestions
    ) as client1:
        client1.expect(prompt)
        with client(name="client2>", log=None, command=command_text) as client2:
            client2.expect(prompt)
            with client(name="client3>", log=None, command=command_text) as client3:
                client3.expect("USER_SESSION_LIMIT_EXCEEDED")

            client1.send("SELECT 'CLIENT_1_SELECT' FORMAT CSV")
            client1.expect("CLIENT_1_SELECT")
            client1.expect(prompt)
            client2.send("SELECT 'CLIENT_2_SELECT' FORMAT CSV")
            client2.expect("CLIENT_2_SELECT")
            client2.expect(prompt)
