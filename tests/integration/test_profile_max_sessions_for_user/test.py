import os
import sys
import threading

import grpc
import psycopg2 as py_psql
import pymysql.connections
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry
from helpers.uclient import client, prompt

script_dir = os.path.dirname(os.path.realpath(__file__))
grpc_protocol_pb2_dir = os.path.join(script_dir, "grpc_protocol_pb2")
if grpc_protocol_pb2_dir not in sys.path:
    sys.path.append(grpc_protocol_pb2_dir)
import clickhouse_grpc_pb2  # Execute grpc_protocol_pb2/generate.py to generate these modules.
import clickhouse_grpc_pb2_grpc

MAX_SESSIONS_FOR_USER = 2
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
        "UBSAN_OPTIONS": "print_stacktrace=1",
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


def grpc_query(query_text, channel, session_id_):
    query_info = clickhouse_grpc_pb2.QueryInfo(
        query=query_text,
        session_id=session_id_,
        user_name=TEST_USER,
        password=TEST_PASSWORD,
    )

    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    result = stub.ExecuteQuery(query_info)
    if result and result.HasField("exception"):
        raise Exception(result.exception.display_text)
    return result.output.decode(DEFAULT_ENCODING)


def threaded_run_test(sessions):
    instance.rotate_logs()
    thread_list = []
    for i in range(len(sessions)):
        thread = ThreadWithException(target=sessions[i], args=(i,))
        thread_list.append(thread)
        thread.start()

    if len(sessions) > MAX_SESSIONS_FOR_USER:
        # High retry amount to avoid flakiness in ASAN (+Analyzer) tests
        assert_logs_contain_with_retry(
            instance, "overflown session count", retry_count=120
        )

    instance.query(f"KILL QUERY WHERE user='{TEST_USER}' SYNC")

    for thread in thread_list:
        thread.join()


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


class ThreadWithException(threading.Thread):
    def run(self):
        try:
            super().run()
        except:
            pass


def postgres_session(id):
    ch = py_psql.connect(
        host=instance.ip_address,
        port=POSTGRES_SERVER_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database="default",
    )
    cur = ch.cursor()
    cur.execute(get_query("postgres_session", id))
    cur.fetchall()


def mysql_session(id):
    client = pymysql.connections.Connection(
        host=instance.ip_address,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database="default",
        port=MYSQL_SERVER_PORT,
    )
    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute(get_query("mysql_session", id))
    cursor.fetchall()


def tcp_session(id):
    instance.query(get_query("tcp_session", id), user=TEST_USER, password=TEST_PASSWORD)


def http_session(id):
    instance.http_query(
        get_query("http_session", id), user=TEST_USER, password=TEST_PASSWORD
    )


def http_named_session(id):
    instance.http_query(
        get_query("http_named_session", id),
        user=TEST_USER,
        password=TEST_PASSWORD,
        params={"session_id": id},
    )


def grpc_session(id):
    grpc_query(
        get_query("grpc_session", id), grpc_create_insecure_channel(), f"session_{id}"
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
