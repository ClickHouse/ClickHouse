import os
import grpc
import pymysql.connections
import pytest
import random
import sys
import threading

from helpers.cluster import ClickHouseCluster, run_and_check

POSTGRES_SERVER_PORT = 5433
MYSQL_SERVER_PORT = 9001
GRPC_PORT = 9100
SESSION_LOG_MATCHING_FIELDS = "auth_id, auth_type, client_version_major, client_version_minor, client_version_patch, interface"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULT_ENCODING = "utf-8"

# Use grpcio-tools to generate *pb2.py files from *.proto.
proto_dir = os.path.join(SCRIPT_DIR, "./protos")
gen_dir = os.path.join(SCRIPT_DIR, "./_gen")
os.makedirs(gen_dir, exist_ok=True)
run_and_check(
    f"python3 -m grpc_tools.protoc -I{proto_dir} --python_out={gen_dir} --grpc_python_out={gen_dir} {proto_dir}/clickhouse_grpc.proto",
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
        "configs/session_log.xml",
    ],
    user_configs=["configs/users.xml"],
    # Bug in TSAN reproduces in this test https://github.com/grpc/grpc/issues/29550#issuecomment-1188085387
    env_variables={
        "TSAN_OPTIONS": "report_atomic_races=0 " + os.getenv("TSAN_OPTIONS", default="")
    },
    with_postgres=True,
)


def grpc_get_url():
    return f"{instance.ip_address}:{GRPC_PORT}"


def grpc_create_insecure_channel():
    channel = grpc.insecure_channel(grpc_get_url())
    grpc.channel_ready_future(channel).result(timeout=2)
    return channel


session_id_counter = 0


def next_session_id():
    global session_id_counter
    session_id = session_id_counter
    session_id_counter += 1
    return str(session_id)


def grpc_query(query, user_, pass_, raise_exception):
    try:
        query_info = clickhouse_grpc_pb2.QueryInfo(
            query=query,
            session_id=next_session_id(),
            user_name=user_,
            password=pass_,
        )
        channel = grpc_create_insecure_channel()
        stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
        result = stub.ExecuteQuery(query_info)
        if result and result.HasField("exception"):
            raise Exception(result.exception.display_text)

        return result.output.decode(DEFAULT_ENCODING)
    except Exception:
        assert raise_exception


def postgres_query(query, user_, pass_, raise_exception):
    try:
        connection_string = f"host={instance.hostname} port={POSTGRES_SERVER_PORT} dbname=default user={user_} password={pass_}"
        cluster.exec_in_container(
            cluster.postgres_id,
            [
                "/usr/bin/psql",
                connection_string,
                "--no-align",
                "--field-separator=' '",
                "-c",
                query,
            ],
            shell=True,
        )
    except Exception:
        assert raise_exception


def mysql_query(query, user_, pass_, raise_exception):
    try:
        client = pymysql.connections.Connection(
            host=instance.ip_address,
            user=user_,
            password=pass_,
            database="default",
            port=MYSQL_SERVER_PORT,
        )
        cursor = client.cursor(pymysql.cursors.DictCursor)
        if raise_exception:
            with pytest.raises(Exception):
                cursor.execute(query)
        else:
            cursor.execute(query)
        cursor.fetchall()
    except Exception:
        assert raise_exception


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


def test_grpc_session(started_cluster):
    grpc_query("SELECT 1", "grpc_user", "pass", False)
    grpc_query("SELECT 2", "grpc_user", "wrong_pass", True)
    grpc_query("SELECT 3", "wrong_grpc_user", "pass", True)

    instance.query("SYSTEM FLUSH LOGS")
    login_success_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='grpc_user' AND type = 'LoginSuccess'"
    )
    assert login_success_records == "grpc_user\t1\t1\n"
    logout_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='grpc_user' AND type = 'Logout'"
    )
    assert logout_records == "grpc_user\t1\t1\n"
    login_failure_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='grpc_user' AND type = 'LoginFailure'"
    )
    assert login_failure_records == "grpc_user\t1\t1\n"
    logins_and_logouts = instance.query(
        f"SELECT COUNT(*) FROM (SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'grpc_user' AND type = 'LoginSuccess' INTERSECT SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'grpc_user' AND type = 'Logout')"
    )
    assert logins_and_logouts == "1\n"


def test_mysql_session(started_cluster):
    mysql_query("SELECT 1", "mysql_user", "pass", False)
    mysql_query("SELECT 2", "mysql_user", "wrong_pass", True)
    mysql_query("SELECT 3", "wrong_mysql_user", "pass", True)

    instance.query("SYSTEM FLUSH LOGS")
    login_success_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='mysql_user' AND type = 'LoginSuccess'"
    )
    assert login_success_records == "mysql_user\t1\t1\n"
    logout_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='mysql_user' AND type = 'Logout'"
    )
    assert logout_records == "mysql_user\t1\t1\n"
    login_failure_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='mysql_user' AND type = 'LoginFailure'"
    )
    assert login_failure_records == "mysql_user\t1\t1\n"
    logins_and_logouts = instance.query(
        f"SELECT COUNT(*) FROM (SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'mysql_user' AND type = 'LoginSuccess' INTERSECT SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'mysql_user' AND type = 'Logout')"
    )
    assert logins_and_logouts == "1\n"


def test_postgres_session(started_cluster):
    postgres_query("SELECT 1", "postgres_user", "pass", False)
    postgres_query("SELECT 2", "postgres_user", "wrong_pass", True)
    postgres_query("SELECT 3", "wrong_postgres_user", "pass", True)

    instance.query("SYSTEM FLUSH LOGS")
    login_success_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='postgres_user' AND type = 'LoginSuccess'"
    )
    assert login_success_records == "postgres_user\t1\t1\n"
    logout_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='postgres_user' AND type = 'Logout'"
    )
    assert logout_records == "postgres_user\t1\t1\n"
    login_failure_records = instance.query(
        "SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='postgres_user' AND type = 'LoginFailure'"
    )
    assert login_failure_records == "postgres_user\t1\t1\n"
    logins_and_logouts = instance.query(
        f"SELECT COUNT(*) FROM (SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'postgres_user' AND type = 'LoginSuccess' INTERSECT SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'postgres_user' AND type = 'Logout')"
    )
    assert logins_and_logouts == "1\n"


def test_parallel_sessions(started_cluster):
    thread_list = []
    for _ in range(10):
        # Sleep time does not significantly matter here,
        # test should pass even without sleeping.
        for function in [postgres_query, grpc_query, mysql_query]:
            thread = threading.Thread(
                target=function,
                args=(
                    f"SELECT sleep({random.uniform(0.03, 0.04)})",
                    "parallel_user",
                    "pass",
                    False,
                ),
            )
            thread.start()
            thread_list.append(thread)
            thread = threading.Thread(
                target=function,
                args=(
                    f"SELECT sleep({random.uniform(0.03, 0.04)})",
                    "parallel_user",
                    "wrong_pass",
                    True,
                ),
            )
            thread.start()
            thread_list.append(thread)
            thread = threading.Thread(
                target=function,
                args=(
                    f"SELECT sleep({random.uniform(0.03, 0.04)})",
                    "wrong_parallel_user",
                    "pass",
                    True,
                ),
            )
            thread.start()
            thread_list.append(thread)

    for thread in thread_list:
        thread.join()

    instance.query("SYSTEM FLUSH LOGS")
    port_0_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = 'parallel_user'"
    )
    assert port_0_sessions == "90\n"

    port_0_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = 'parallel_user' AND client_port = 0"
    )
    assert port_0_sessions == "0\n"

    address_0_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = 'parallel_user' AND client_address = toIPv6('::')"
    )
    assert address_0_sessions == "0\n"

    grpc_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = 'parallel_user' AND interface = 'gRPC'"
    )
    assert grpc_sessions == "30\n"

    mysql_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = 'parallel_user' AND interface = 'MySQL'"
    )
    assert mysql_sessions == "30\n"

    postgres_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = 'parallel_user' AND interface = 'PostgreSQL'"
    )
    assert postgres_sessions == "30\n"

    logins_and_logouts = instance.query(
        f"SELECT COUNT(*) FROM (SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'parallel_user' AND type = 'LoginSuccess' INTERSECT SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = 'parallel_user' AND type = 'Logout')"
    )
    assert logins_and_logouts == "30\n"

    logout_failure_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log  WHERE user = 'parallel_user' AND type = 'LoginFailure'"
    )
    assert logout_failure_sessions == "30\n"
