import os

import grpc
import pymysql.connections
import psycopg2 as py_psql
import pytest
import sys
import threading

from helpers.cluster import ClickHouseCluster, run_and_check

MAX_SESSIONS_FOR_USER = 2
POSTGRES_SERVER_PORT = 5433
MYSQL_SERVER_PORT = 9001
GRPC_PORT = 9100

TEST_USER = "test_user"
TEST_PASSWORD = "123"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULT_ENCODING = "utf-8"

# Use grpcio-tools to generate *pb2.py files from *.proto.
proto_dir = os.path.join(SCRIPT_DIR, "./protos")
gen_dir = os.path.join(SCRIPT_DIR, "./_gen")
os.makedirs(gen_dir, exist_ok=True)
run_and_check(
    "python3 -m grpc_tools.protoc -I{proto_dir} --python_out={gen_dir} --grpc_python_out={gen_dir} \
    {proto_dir}/clickhouse_grpc.proto".format(
        proto_dir=proto_dir, gen_dir=gen_dir
    ),
    shell=True,
)

sys.path.append(gen_dir)

import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

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
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)


def get_query(name, id):
    return f"SElECT '{name}', {id}, sleep(1)"


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
    thread_list = []
    for i in range(len(sessions)):
        thread = ThreadWithException(target=sessions[i], args=(i,))
        thread_list.append(thread)
        thread.start()

    for thread in thread_list:
        thread.join()

    exception_count = 0
    for i in range(len(sessions)):
        if thread_list[i].run_exception != None:
            exception_count += 1

    assert exception_count == 1


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class ThreadWithException(threading.Thread):
    run_exception = None

    def run(self):
        try:
            super().run()
        except:
            self.run_exception = sys.exc_info()

    def join(self):
        super().join()


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


def test_profile_max_sessions_for_user_end_session(started_cluster):
    for conection_func in [
        tcp_session,
        http_session,
        grpc_session,
        mysql_session,
        postgres_session,
    ]:
        threaded_run_test([conection_func] * MAX_SESSIONS_FOR_USER)
        threaded_run_test([conection_func] * MAX_SESSIONS_FOR_USER)


def test_profile_max_sessions_for_user_end_session(started_cluster):
    instance.query_and_get_error("SET max_sessions_for_user = 10")
