import contextlib
import grpc
import psycopg2
import pymysql.connections
import pymysql.err
import pytest
import sys
import time
from helpers.cluster import ClickHouseCluster, run_and_check
from helpers.client import Client, QueryRuntimeException
from kazoo.exceptions import NodeExistsError
from pathlib import Path
from requests.exceptions import ConnectionError
from urllib3.util.retry import Retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/ports_from_zk.xml", "configs/ssl_conf.xml", "configs/dhparam.pem", "configs/server.crt", "configs/server.key"
    ],
    user_configs=["configs/default_passwd.xml"],
    with_zookeeper=True)


LOADS_QUERY = "SELECT value FROM system.events WHERE event = 'MainConfigLoads'"


# Use grpcio-tools to generate *pb2.py files from *.proto.

proto_dir = Path(__file__).parent / "protos"
gen_dir = Path(__file__).parent / "_gen"
gen_dir.mkdir(exist_ok=True)
run_and_check(
    f"python3 -m grpc_tools.protoc -I{proto_dir!s} --python_out={gen_dir!s} --grpc_python_out={gen_dir!s} \
    {proto_dir!s}/clickhouse_grpc.proto", shell=True)

sys.path.append(str(gen_dir))
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc


@pytest.fixture(name="cluster", scope="module")
def fixture_cluster():
    try:
        cluster.add_zookeeper_startup_command(configure_ports_from_zk)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(name="zk", scope="module")
def fixture_zk(cluster):
    return cluster.get_kazoo_client("zoo1")


def get_client(cluster, port):
    return Client(host=cluster.get_instance_ip("instance"), port=port, command=cluster.client_bin_path)


def get_mysql_client(cluster, port):
    start_time = time.monotonic()
    while True:
        try:
            return pymysql.connections.Connection(
                host=cluster.get_instance_ip("instance"), user="default", password="", database="default", port=port)
        except pymysql.err.OperationalError:
            if time.monotonic() - start_time > 10:
                raise
            time.sleep(0.1)


def get_pgsql_client(cluster, port):
    start_time = time.monotonic()
    while True:
        try:
            return psycopg2.connect(
                host=cluster.get_instance_ip("instance"), user="postgresql", password="123", database="default", port=port)
        except psycopg2.OperationalError:
            if time.monotonic() - start_time > 10:
                raise
            time.sleep(0.1)


def get_grpc_channel(cluster, port):
    host_port = cluster.get_instance_ip("instance") + f":{port}"
    channel = grpc.insecure_channel(host_port)
    grpc.channel_ready_future(channel).result(timeout=10)
    return channel


def grpc_query(channel, query_text):
    query_info = clickhouse_grpc_pb2.QueryInfo(query=query_text)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    result = stub.ExecuteQuery(query_info)
    if result and result.HasField("exception"):
        raise Exception(result.exception.display_text)
    return result.output.decode()


def configure_ports_from_zk(zk, querier=None):
    default_config = [
        ("/clickhouse/listen_hosts", b"<listen_host>0.0.0.0</listen_host>"),
        ("/clickhouse/ports/tcp", b"9000"),
        ("/clickhouse/ports/http", b"8123"),
        ("/clickhouse/ports/mysql", b"9004"),
        ("/clickhouse/ports/postgresql", b"9005"),
        ("/clickhouse/ports/grpc", b"9100"),
    ]
    for path, value in default_config:
        if querier is not None:
            loads_before = querier(LOADS_QUERY)
        has_changed = False
        try:
            zk.create(path=path, value=value, makepath=True)
            has_changed = True
        except NodeExistsError:
            if zk.get(path) != value:
                zk.set(path=path, value=value)
                has_changed = True
        if has_changed and querier is not None:
            wait_loaded_config_changed(loads_before, querier)


@contextlib.contextmanager
def sync_loaded_config(querier):
    # Depending on whether we test a change on tcp or http
    # we monitor canges using the other, untouched, protocol
    loads_before = querier(LOADS_QUERY)
    yield
    wait_loaded_config_changed(loads_before, querier)


def wait_loaded_config_changed(loads_before, querier):
    loads_after = None
    start_time = time.monotonic()
    while time.monotonic() - start_time < 10:
        try:
            loads_after = querier(LOADS_QUERY)
            if loads_after != loads_before:
                return
        except (QueryRuntimeException, ConnectionError):
            pass
        time.sleep(0.1)
    assert loads_after is not None and loads_after != loads_before


@contextlib.contextmanager
def default_client(cluster, zk, restore_via_http=False):
    client = get_client(cluster, port=9000)
    try:
        yield client
    finally:
        querier = instance.http_query if restore_via_http else client.query
        configure_ports_from_zk(zk, querier)


def test_change_tcp_port(cluster, zk):
    with default_client(cluster, zk, restore_via_http=True) as client:
        assert client.query("SELECT 1") == "1\n"
        with sync_loaded_config(instance.http_query):
            zk.set("/clickhouse/ports/tcp", b"9090")
        with pytest.raises(QueryRuntimeException, match="Connection refused"):
            client.query("SELECT 1")
        client_on_new_port = get_client(cluster, port=9090)
        assert client_on_new_port.query("SELECT 1") == "1\n"


def test_change_http_port(cluster, zk):
    with default_client(cluster, zk) as client:
        retry_strategy = Retry(total=10, backoff_factor=0.1)
        assert instance.http_query("SELECT 1", retry_strategy=retry_strategy) == "1\n"
        with sync_loaded_config(client.query):
            zk.set("/clickhouse/ports/http", b"9090")
        with pytest.raises(ConnectionError, match="Connection refused"):
            instance.http_query("SELECT 1")
        instance.http_query("SELECT 1", port=9090) == "1\n"


def test_change_mysql_port(cluster, zk):
    with default_client(cluster, zk) as client:
        mysql_client = get_mysql_client(cluster, port=9004)
        assert mysql_client.query("SELECT 1") == 1
        with sync_loaded_config(client.query):
            zk.set("/clickhouse/ports/mysql", b"9090")
        with pytest.raises(pymysql.err.OperationalError, match="Lost connection"):
            mysql_client.query("SELECT 1")
        mysql_client_on_new_port = get_mysql_client(cluster, port=9090)
        assert mysql_client_on_new_port.query("SELECT 1") == 1


def test_change_postgresql_port(cluster, zk):
    with default_client(cluster, zk) as client:
        pgsql_client = get_pgsql_client(cluster, port=9005)
        cursor = pgsql_client.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchall() == [(1,)]
        with sync_loaded_config(client.query):
            zk.set("/clickhouse/ports/postgresql", b"9090")
        with pytest.raises(psycopg2.OperationalError, match="closed"):
            cursor.execute("SELECT 1")
        pgsql_client_on_new_port = get_pgsql_client(cluster, port=9090)
        cursor = pgsql_client_on_new_port.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall() == [(1,)]


def test_change_grpc_port(cluster, zk):
    with default_client(cluster, zk) as client:
        grpc_channel = get_grpc_channel(cluster, port=9100)
        assert grpc_query(grpc_channel, "SELECT 1") == "1\n"
        with sync_loaded_config(client.query):
            zk.set("/clickhouse/ports/grpc", b"9090")
        with pytest.raises(grpc._channel._InactiveRpcError, match="StatusCode.UNAVAILABLE"):
            grpc_query(grpc_channel, "SELECT 1")
        grpc_channel_on_new_port = get_grpc_channel(cluster, port=9090)
        assert grpc_query(grpc_channel_on_new_port, "SELECT 1") == "1\n"


def test_remove_tcp_port(cluster, zk):
    with default_client(cluster, zk, restore_via_http=True) as client:
        assert client.query("SELECT 1") == "1\n"
        with sync_loaded_config(instance.http_query):
            zk.delete("/clickhouse/ports/tcp")
        with pytest.raises(QueryRuntimeException, match="Connection refused"):
            client.query("SELECT 1")


def test_remove_http_port(cluster, zk):
    with default_client(cluster, zk) as client:
        assert instance.http_query("SELECT 1") == "1\n"
        with sync_loaded_config(client.query):
            zk.delete("/clickhouse/ports/http")
        with pytest.raises(ConnectionError, match="Connection refused"):
            instance.http_query("SELECT 1")


def test_remove_mysql_port(cluster, zk):
    with default_client(cluster, zk) as client:
        mysql_client = get_mysql_client(cluster, port=9004)
        assert mysql_client.query("SELECT 1") == 1
        with sync_loaded_config(client.query):
            zk.delete("/clickhouse/ports/mysql")
        with pytest.raises(pymysql.err.OperationalError, match="Lost connection"):
            mysql_client.query("SELECT 1")


def test_remove_postgresql_port(cluster, zk):
    with default_client(cluster, zk) as client:
        pgsql_client = get_pgsql_client(cluster, port=9005)
        cursor = pgsql_client.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchall() == [(1,)]
        with sync_loaded_config(client.query):
            zk.delete("/clickhouse/ports/postgresql")
        with pytest.raises(psycopg2.OperationalError, match="closed"):
            cursor.execute("SELECT 1")


def test_remove_grpc_port(cluster, zk):
    with default_client(cluster, zk) as client:
        grpc_channel = get_grpc_channel(cluster, port=9100)
        assert grpc_query(grpc_channel, "SELECT 1") == "1\n"
        with sync_loaded_config(client.query):
            zk.delete("/clickhouse/ports/grpc")
        with pytest.raises(grpc._channel._InactiveRpcError, match="StatusCode.UNAVAILABLE"):
            grpc_query(grpc_channel, "SELECT 1")


def test_change_listen_host(cluster, zk):
    localhost_client = Client(host="127.0.0.1", port=9000, command="/usr/bin/clickhouse")
    localhost_client.command = ["docker", "exec", "-i", instance.docker_id] + localhost_client.command
    try:
        client = get_client(cluster, port=9000)
        with sync_loaded_config(localhost_client.query):
            zk.set("/clickhouse/listen_hosts", b"<listen_host>127.0.0.1</listen_host>")
        with pytest.raises(QueryRuntimeException, match="Connection refused"):
            client.query("SELECT 1")
        assert localhost_client.query("SELECT 1") == "1\n"
    finally:
        with sync_loaded_config(localhost_client.query):
            configure_ports_from_zk(zk)

