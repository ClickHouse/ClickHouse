# pylint: disable=wrong-import-order
# pylint: disable=line-too-long
# pylint: disable=redefined-builtin
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access
# pylint: disable=broad-except

import contextlib
import logging
import os
import sys
import time
from pathlib import Path

import grpc
import psycopg2
import pymysql.connections
import pymysql.err
import pytest
from kazoo.exceptions import NodeExistsError
from requests.exceptions import ConnectionError
from urllib3.util.retry import Retry

from helpers.client import Client, QueryRuntimeException
from helpers.cluster import ClickHouseCluster, run_and_check

script_dir = os.path.dirname(os.path.realpath(__file__))
grpc_protocol_pb2_dir = os.path.join(script_dir, "grpc_protocol_pb2")
if grpc_protocol_pb2_dir not in sys.path:
    sys.path.append(grpc_protocol_pb2_dir)
import clickhouse_grpc_pb2  # Execute grpc_protocol_pb2/generate.py to generate these modules.
import clickhouse_grpc_pb2_grpc

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/overrides_from_zk.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=["configs/default_passwd.xml"],
    with_zookeeper=True,
    # Bug in TSAN reproduces in this test https://github.com/grpc/grpc/issues/29550#issuecomment-1188085387
    env_variables={
        "TSAN_OPTIONS": "report_atomic_races=0 " + os.getenv("TSAN_OPTIONS", default="")
    },
)


LOADS_QUERY = "SELECT value FROM system.events WHERE event = 'MainConfigLoads'"


@pytest.fixture(name="cluster", scope="module")
def fixture_cluster():
    try:
        cluster.add_zookeeper_startup_command(configure_from_zk)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(name="zk", scope="module")
def fixture_zk(cluster):
    return cluster.get_kazoo_client("zoo1")


def get_client(cluster, port):
    return Client(
        host=cluster.get_instance_ip("instance"),
        port=port,
        command=cluster.client_bin_path,
    )


def get_mysql_client(cluster, port):
    start_time = time.monotonic()
    while True:
        try:
            return pymysql.connections.Connection(
                host=cluster.get_instance_ip("instance"),
                user="default",
                password="",
                database="default",
                port=port,
            )
        except pymysql.err.OperationalError:
            if time.monotonic() - start_time > 10:
                raise
            time.sleep(0.1)


def get_pgsql_client(cluster, port):
    start_time = time.monotonic()
    while True:
        try:
            return psycopg2.connect(
                host=cluster.get_instance_ip("instance"),
                user="postgresql",
                password="123",
                database="default",
                port=port,
            )
        except psycopg2.OperationalError:
            if time.monotonic() - start_time > 10:
                raise
            time.sleep(0.1)


@contextlib.contextmanager
def get_grpc_channel(cluster, port):
    host_port = cluster.get_instance_ip("instance") + f":{port}"
    channel = grpc.insecure_channel(host_port)
    grpc.channel_ready_future(channel).result(timeout=10)
    try:
        yield channel
    finally:
        channel.close()


def grpc_query(channel, query_text):
    query_info = clickhouse_grpc_pb2.QueryInfo(query=query_text)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    result = stub.ExecuteQuery(query_info)
    if result and result.HasField("exception"):
        raise Exception(result.exception.display_text)
    return result.output.decode()


def configure_from_zk(zk, querier=None):
    default_config = [
        ("/clickhouse/listen_hosts", b"<listen_host>0.0.0.0</listen_host>"),
        ("/clickhouse/ports/tcp", b"9000"),
        ("/clickhouse/ports/http", b"8123"),
        ("/clickhouse/ports/mysql", b"9004"),
        ("/clickhouse/ports/postgresql", b"9005"),
        ("/clickhouse/ports/grpc", b"9100"),
        ("/clickhouse/http_handlers", b"<defaults/>"),
    ]
    for path, value in default_config:
        if querier is not None:
            loads_before = querier(LOADS_QUERY)
        has_changed = False
        try:
            zk.create(path=path, value=value, makepath=True)
            has_changed = True
        except NodeExistsError:
            if zk.get(path)[0] != value:
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
        configure_from_zk(zk, querier)


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
        assert instance.http_query("SELECT 1", port=9090) == "1\n"


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
        assert cursor.fetchall() == [(1,)]


def test_change_grpc_port(cluster, zk):
    with default_client(cluster, zk) as client:
        with get_grpc_channel(cluster, port=9100) as grpc_channel:
            assert grpc_query(grpc_channel, "SELECT 1") == "1\n"
            with sync_loaded_config(client.query):
                zk.set("/clickhouse/ports/grpc", b"9090")
            with pytest.raises(
                grpc._channel._InactiveRpcError, match="StatusCode.UNAVAILABLE"
            ):
                grpc_query(grpc_channel, "SELECT 1")

        with get_grpc_channel(cluster, port=9090) as grpc_channel_on_new_port:
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
        with get_grpc_channel(cluster, port=9100) as grpc_channel:
            assert grpc_query(grpc_channel, "SELECT 1") == "1\n"
            with sync_loaded_config(client.query):
                zk.delete("/clickhouse/ports/grpc")
            with pytest.raises(
                grpc._channel._InactiveRpcError, match="StatusCode.UNAVAILABLE"
            ):
                grpc_query(grpc_channel, "SELECT 1")


def test_change_listen_host(cluster, zk):
    localhost_client = Client(
        host="127.0.0.1", port=9000, command="/usr/bin/clickhouse"
    )
    localhost_client.command = [
        "docker",
        "exec",
        "-i",
        instance.docker_id,
    ] + localhost_client.command
    try:
        client = get_client(cluster, port=9000)
        with sync_loaded_config(localhost_client.query):
            zk.set("/clickhouse/listen_hosts", b"<listen_host>127.0.0.1</listen_host>")
        with pytest.raises(QueryRuntimeException, match="Connection refused"):
            client.query("SELECT 1")
        assert localhost_client.query("SELECT 1") == "1\n"
    finally:
        with sync_loaded_config(localhost_client.query):
            configure_from_zk(zk)


# This is a regression test for the case when the clickhouse-server was waiting
# for the connection that had been issued "SYSTEM RELOAD CONFIG" indefinitely.
#
# Configuration reload directly from the query,
# "directly from the query" means that reload was done from the query context
# over periodic config reload (that is done each 2 seconds).
def test_reload_via_client(cluster, zk):
    exception = None

    localhost_client = Client(
        host="127.0.0.1", port=9000, command="/usr/bin/clickhouse"
    )
    localhost_client.command = [
        "docker",
        "exec",
        "-i",
        instance.docker_id,
    ] + localhost_client.command

    # NOTE: reload via zookeeper is too fast, but 100 iterations was enough, even for debug build.
    for i in range(0, 100):
        try:
            client = get_client(cluster, port=9000)
            zk.set("/clickhouse/listen_hosts", b"<listen_host>127.0.0.1</listen_host>")
            query_id = f"reload_config_{i}"
            client.query("SYSTEM RELOAD CONFIG", query_id=query_id)
            assert int(localhost_client.query("SELECT 1")) == 1
            localhost_client.query("SYSTEM FLUSH LOGS")
            MainConfigLoads = int(
                localhost_client.query(
                    f"""
            SELECT ProfileEvents['MainConfigLoads']
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            """
                )
            )
            assert MainConfigLoads == 1
            logging.info("MainConfigLoads = %s (retry %s)", MainConfigLoads, i)
            exception = None
            break
        except Exception as e:
            logging.exception("Retry %s", i)
            exception = e
        finally:
            while True:
                try:
                    with sync_loaded_config(localhost_client.query):
                        configure_from_zk(zk)
                    break
                except QueryRuntimeException:
                    logging.exception("The new socket is not binded yet")
                    time.sleep(0.1)

    if exception:
        raise exception


def test_change_http_handlers(cluster, zk):
    with default_client(cluster, zk) as client:
        curl_result = instance.exec_in_container(
            ["bash", "-c", "curl -s '127.0.0.1:8123/it_works'"]
        )
        assert "There is no handle /it_works" in curl_result

        with sync_loaded_config(client.query):
            zk.set(
                "/clickhouse/http_handlers",
                b"""
                <defaults/>

                <rule>
                    <url>/it_works</url>
                    <methods>GET</methods>
                    <handler>
                        <type>predefined_query_handler</type>
                        <query>SELECT 'It works.'</query>
                    </handler>
                </rule>
            """,
            )

        curl_result = instance.exec_in_container(
            ["bash", "-c", "curl -s '127.0.0.1:8123/it_works'"]
        )
        assert curl_result == "It works.\n"
