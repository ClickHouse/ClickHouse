import os
import grpc
import pymysql.connections
import pytest
import random
import sys
import threading
import time

from helpers.cluster import ClickHouseCluster, run_and_check

script_dir = os.path.dirname(os.path.realpath(__file__))
grpc_protocol_pb2_dir = os.path.join(script_dir, "grpc_protocol_pb2")
if grpc_protocol_pb2_dir not in sys.path:
    sys.path.append(grpc_protocol_pb2_dir)
import clickhouse_grpc_pb2, clickhouse_grpc_pb2_grpc  # Execute grpc_protocol_pb2/generate.py to generate these modules.


POSTGRES_SERVER_PORT = 5433
MYSQL_SERVER_PORT = 9001
GRPC_PORT = 9100
SESSION_LOG_MATCHING_FIELDS = "auth_id, auth_type, client_version_major, client_version_minor, client_version_patch, interface"
DEFAULT_ENCODING = "utf-8"


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


user_counter = 0


def create_unique_user(prefix):
    global user_counter
    user_counter += 1
    user_name = f"{prefix}_{os.getppid()}_{user_counter}"
    instance.query(
        f"CREATE USER {user_name} IDENTIFIED WITH plaintext_password BY 'pass'"
    )
    return user_name


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


def wait_for_corresponding_login_success_and_logout(user, expected_login_count):
    # The client can exit sooner than the server records its disconnection and closes the session.
    # When the client disconnects, two processes happen at the same time and are in the race condition:
    # - the client application exits and returns control to the shell;
    # - the server closes the session and records the logout event to the session log.
    # We cannot expect that after the control is returned to the shell, the server records the logout event.
    sql = f"SELECT COUNT(*) FROM (SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = '{user}' AND type = 'LoginSuccess' INTERSECT SELECT {SESSION_LOG_MATCHING_FIELDS} FROM system.session_log WHERE user = '{user}' AND type = 'Logout')"
    logins_and_logouts = instance.query(sql)
    while int(logins_and_logouts) != expected_login_count:
        time.sleep(0.1)
        logins_and_logouts = instance.query(sql)


def check_session_log(user):
    instance.query("SYSTEM FLUSH LOGS")
    login_success_records = instance.query(
        f"SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='{user}' AND type = 'LoginSuccess'"
    )
    assert login_success_records == f"{user}\t1\t1\n"
    logout_records = instance.query(
        f"SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='{user}' AND type = 'Logout'"
    )
    assert logout_records == f"{user}\t1\t1\n"
    login_failure_records = instance.query(
        f"SELECT user, client_port <> 0,  client_address <> toIPv6('::') FROM system.session_log WHERE user='{user}' AND type = 'LoginFailure'"
    )
    assert login_failure_records == f"{user}\t1\t1\n"

    wait_for_corresponding_login_success_and_logout(user, 1)


def session_log_test(prefix, query_function):
    user = create_unique_user(prefix)
    wrong_user = "wrong_" + user

    query_function("SELECT 1", user, "pass", False)
    query_function("SELECT 2", user, "wrong_pass", True)
    query_function("SELECT 3", wrong_user, "pass", True)

    check_session_log(user)

    instance.query(f"DROP USER {user}")


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
    session_log_test("grpc", grpc_query)


def test_mysql_session(started_cluster):
    session_log_test("mysql", mysql_query)


def test_postgres_session(started_cluster):
    session_log_test("postgres", postgres_query)


def test_parallel_sessions(started_cluster):
    user = create_unique_user("parallel")
    wrong_user = "wrong_" + user

    thread_list = []
    for _ in range(10):
        # Sleep time does not significantly matter here,
        # test should pass even without sleeping.
        for function in [postgres_query, grpc_query, mysql_query]:
            thread = threading.Thread(
                target=function,
                args=(
                    f"SELECT sleep({random.uniform(0.03, 0.04)})",
                    user,
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
                    user,
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
                    wrong_user,
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
        f"SELECT COUNT(*) FROM system.session_log WHERE user = '{user}'"
    )
    assert port_0_sessions == "90\n"

    port_0_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = '{user}' AND client_port = 0"
    )
    assert port_0_sessions == "0\n"

    address_0_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = '{user}' AND client_address = toIPv6('::')"
    )
    assert address_0_sessions == "0\n"

    grpc_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = '{user}' AND interface = 'gRPC'"
    )
    assert grpc_sessions == "30\n"

    mysql_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = '{user}' AND interface = 'MySQL'"
    )
    assert mysql_sessions == "30\n"

    postgres_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log WHERE user = '{user}' AND interface = 'PostgreSQL'"
    )
    assert postgres_sessions == "30\n"

    wait_for_corresponding_login_success_and_logout(user, 30)

    logout_failure_sessions = instance.query(
        f"SELECT COUNT(*) FROM system.session_log  WHERE user = '{user}' AND type = 'LoginFailure'"
    )
    assert logout_failure_sessions == "30\n"
